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

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.QueryStreamRgx
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * All credit for this workload goes to the work by Xin Wu and his team while making their TPC-DS
 * journey. https://github.com/xwu0226/tpcds-journey-notebook
 */

/**
 * tried to use a Seq[Int] for queries, but got following exception after app finished logging config
 * Exception in thread "main" java.lang.RuntimeException: Unsupported literal type class scala.collection.mutable.ArraySeq ArraySeq(1, 99)
	at org.apache.spark.sql.catalyst.expressions.Literal$.apply(literals.scala:77)
	at org.apache.spark.sql.catalyst.expressions.Literal$$anonfun$create$2.apply(literals.scala:163)
	at org.apache.spark.sql.catalyst.expressions.Literal$$anonfun$create$2.apply(literals.scala:163)
	at scala.util.Try.getOrElse(Try.scala:79)
	at org.apache.spark.sql.catalyst.expressions.Literal$.create(literals.scala:162)
	at org.apache.spark.sql.functions$.typedLit(functions.scala:113)
	at org.apache.spark.sql.functions$.lit(functions.scala:96)
	at com.ibm.sparktc.sparkbench.utils.SparkFuncs$$anonfun$addConfToResults$1.apply(SparkFuncs.scala:81)
 */

case class TpcDsQueryStats(queryName: String, duration: Long, resultLengh: Int)
case class TpcDsQueryInfo(queryNum: Int, streamNum: Int, queryTemplate: String, queries: Seq[String])

trait QueryState
case object QueryStateNone extends QueryState
case object QueryStateScanning extends QueryState

object TpcDsWorkload extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  val name = "tpcds"

  override def apply(m: Map[String, Any]): Workload = {
    TpcDsWorkload(
      optionallyGet(m, "input"),
      optionallyGet(m, "output"),
      getOrThrowT[String](m, "querystream"),
      getOrDefault[String](m, "dbname", "tpcds")
    )
  }
}

case class TpcDsWorkload(
    input: Option[String],
    output: Option[String],
    queryStream: String,
    dbName: String
  ) extends Workload {
  import TpcDsWorkload._

  private[tpcds] def extractQueries: Seq[TpcDsQueryInfo] = {
    val is = getClass.getResourceAsStream(queryStream)
    val initialState = (QueryStateNone: QueryState, Option.empty[TpcDsQueryInfo], Seq.empty[TpcDsQueryInfo])
    val res = scala.io.Source.fromInputStream(is).getLines.foldLeft(initialState) {
      case ((state, _, queryInfo), line) if state == QueryStateNone && line.startsWith("-- start query")=>
        val QueryStreamRgx(queryNum, streamNum, queryTemplate) = line
        (QueryStateScanning, Some(TpcDsQueryInfo(queryNum.toInt, streamNum.toInt, queryTemplate, Seq.empty)), queryInfo)
      case ((state, qs, queryInfo), line) if state == QueryStateScanning && line.startsWith("-- end query") =>
        (QueryStateNone, None, queryInfo ++ qs)
      case ((state, qs, queryInfo), line) if state == QueryStateScanning =>
        (QueryStateScanning, qs.map { q => q.copy(queries = q.queries :+ line) }, queryInfo)
    }
    res._3
  }

  private def runQuery(queryInfo: TpcDsQueryInfo)(implicit spark: SparkSession): Seq[TpcDsQueryStats] = {
    val queries = queryInfo.queries.mkString(" ").split(";")
    if (queries.isEmpty) throw new Exception(s"No queries to run for $queryStream")
    log.error(s"Running TPC-DS Query ${queryInfo.queryTemplate}")

    queries.map { query =>
      val (dur, result) = time(spark.sql(query).collect)
      TpcDsQueryStats(queryInfo.queryTemplate, dur, result.length)
    }
  }

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    implicit val explicitlyDeclaredImplicitSpark: SparkSession = spark
    spark.sql(s"USE $dbName")
    val queryStats = extractQueries.map { queryInfo => (queryInfo.queryNum, runQuery(queryInfo)) }
    spark.createDataFrame(spark.sparkContext.parallelize(queryStats))
  }
}
