/**
  * (C) Copyright IBM Corp. 2015 - 2017
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package com.ibm.sparktc.sparkbench.workload.tpcds

import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.{ExecutionContext, Future}

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.{loadFile, tables}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class TpcDsQueryStats(queryName: String, queryNum: Int, queryIdx: Int, duration: Long, resultLengh: Int)
case class TpcDsQueryInfo(queryNum: Int, streamNum: Int, queryTemplate: String, queries: Seq[String])

trait QueryState
case object QueryStateNone extends QueryState
case object QueryStateScanning extends QueryState

object TpcDsWorkload extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  val name = "tpcds"
  private val QueryStreamRgx = """^-- start query ([0-9]+) in stream ([0-9]+) using template (query[0-9]+\.tpl).*$""".r

  override def apply(m: Map[String, Any]): Workload = {
    TpcDsWorkload(
      optionallyGet(m, "input"),
      optionallyGet(m, "output"),
      getOrThrowT[String](m, "querystream"),
      getOrDefault[String](m, "dbname", "tpcds"),
      getOrDefault[Boolean](m, "createtemptables", false),
      getOrDefault[Int](m, "parallelqueriestorun", 1)
    )
  }
}

case class TpcDsWorkload(
    input: Option[String],
    output: Option[String],
    queryStream: String,
    dbName: String,
    createTempTables: Boolean,
    parallelQueriesToRun: Int
  ) extends Workload {
  import TpcDsWorkload._

  private[tpcds] def extractQueries(): Seq[TpcDsQueryInfo] = {
    val initialState = (QueryStateNone: QueryState, Option.empty[TpcDsQueryInfo], Seq.empty[TpcDsQueryInfo])
    loadFile(queryStream).get.foldLeft(initialState) {
      case ((QueryStateNone, _, queryInfo), line) if line.startsWith("-- start query") =>
        val QueryStreamRgx(queryNum, streamNum, queryTemplate) = line
        (QueryStateScanning, Some(TpcDsQueryInfo(queryNum.toInt, streamNum.toInt, queryTemplate, Seq.empty)), queryInfo)
      case ((QueryStateScanning, qs, queryInfo), line) if line.startsWith("-- end query") =>
        (QueryStateNone, None, queryInfo ++ qs)
      case ((QueryStateScanning, qs, queryInfo), line) =>
        (QueryStateScanning, qs.map { q => q.copy(queries = q.queries :+ line) }, queryInfo)
    }._3
  }

  private[tpcds] def runQuery(queryInfo: TpcDsQueryInfo)(implicit spark: SparkSession): Seq[TpcDsQueryStats] = {
    val queries = queryInfo.queries
      .mkString(" ")
      .split(";")
      .map(_.replace(";", ""))
      .map(_.trim)
      .filter(_.nonEmpty)

    if (queries.isEmpty) throw new Exception(s"No queries to run for $queryStream")
    log.error(s"Running TPC-DS Query ${queryInfo.queryTemplate}")

    queries.zipWithIndex.map { case (query, i) =>
      log.error(s"Running query $i from ${queryInfo.queryTemplate}:\n$query")
      val (dur, result) = time(spark.sql(query).collect)
      TpcDsQueryStats(queryInfo.queryTemplate, queryInfo.queryNum, i, dur, result.length)
    }
  }

  private[tpcds] def setup()(implicit spark: SparkSession): Unit = {
    if (createTempTables) {
      val warehouseDir = spark.sparkContext.getConf.get("spark.sql.warehouse.dir", "spark-warehouse")
      tables.foreach { t =>
        spark.read.parquet(s"$warehouseDir/$dbName.db/$t").createOrReplaceTempView(t)
      }
    } else spark.sql(s"USE $dbName")
  }

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    implicit val explicitlyDeclaredImplicitSpark: SparkSession = spark
    setup()
    val queries = extractQueries()
    implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(math.min(parallelQueriesToRun, queries.length)))
    val queryStatsFutures = queries.map { queryInfo => Future(runQuery(queryInfo)) }
    val queryStats = waitForFutures(queryStatsFutures).flatten
    spark.createDataFrame(spark.sparkContext.parallelize(queryStats))
  }
}
