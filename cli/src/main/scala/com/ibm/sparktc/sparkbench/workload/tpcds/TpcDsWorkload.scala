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

/**
 * All credit for this workload goes to the work by Xin Wu and his team while making their TPC-DS
 * journey. https://github.com/xwu0226/tpcds-journey-notebook
 */
package com.ibm.sparktc.sparkbench.workload.tpcds

import scala.util.{Try, Success, Failure}
import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TpcDsWorkload extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  val name = "tpcds"

  private def mkQueries(m: Map[String, Any]): Option[Seq[Int]] = {
    // TODO: replace with for-comprehension?
    getOrDefault[Option[String]](m, "queries", None).flatMap {
      case qs if qs.nonEmpty => Some(
        qs.split(",").flatMap { v =>
          Try(v.toInt) match {
            case Success(u) => Some(u)
            case Failure(e) => log.error(s"Failed to parse $v", e); None
          }
        }
      )
      case _ => None
    }
  }

  override def apply(m: Map[String, Any]): Workload = TpcDsWorkload(
    input = None,
    output = None,
    dataDir = getOrThrow(m, "datadir").asInstanceOf[String],
    queries = mkQueries(m)
  )
}

case class TpcDsQueryStats(queryName: String, duration: Long, resultLengh: Int)

case class TpcDsWorkload(
    input: Option[String],
    output: Option[String],
    dataDir: String,
    queries: Option[Seq[Int]]
  ) extends TpcDsBase(dataDir)
    with Workload {
  import TpcDsWorkload._

  private def runQuery(queryNum: Int)(implicit spark: SparkSession): TpcDsQueryStats = {
    val queryStr = "%02d".format(queryNum)
    val queryName = s"hdfs:///$tpcdsQueriesDir/query$queryStr.sql"
    val (_, content) = spark.sparkContext.wholeTextFiles(queryName).collect()(0)
    val queries = content.split("\n").filterNot(_.startsWith("--")).mkString(" ").split(";")
    if (queries.isE) throw new Exception(s"No queries to run for $queryName - ")
    log.error(s"Running TPC-DS Query $queryName")
    queries.map { query =>
      val (dur, result) = time(spark.sql(query).collect)
      TpcDsQueryStats(queryName, dur, result.length)
    }.head
  }

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    implicit val explicitlyDeclaredImplicitSpark: SparkSession = spark
    spark.sql(s"USE $tpcdsDatabaseName")
    val queryStats = queries.getOrElse(1 until 100).map { i => (i, runQuery(i)) }
    spark.createDataFrame(spark.sparkContext.parallelize(queryStats))
  }
}
