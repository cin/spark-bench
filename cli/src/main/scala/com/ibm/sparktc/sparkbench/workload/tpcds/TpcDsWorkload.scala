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

import scala.util.{Try, Success, Failure}
import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase
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

object TpcDsWorkload extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  val name = "tpcds"

  private def mkQueries(q: Option[String]): Seq[Int] = q match {
    case Some(qstr) if qstr.nonEmpty => qstr.split(",").flatMap { v =>
      Try(v.toInt) match {
        case Success(u) => Some(u)
        case Failure(e) => log.error(s"Failed to parse $v", e); None
      }
    }
    case _ => 1 until 100
  }

  override def apply(m: Map[String, Any]): Workload = {
    TpcDsWorkload(
      optionallyGet(m, "input"),
      optionallyGet(m, "output"),
      getOrThrowT(m, "datadir"),
      optionallyGet(m, "queries")
    )
  }
}

case class TpcDsQueryStats(queryName: String, duration: Long, resultLengh: Int)

case class TpcDsWorkload(
    input: Option[String],
    output: Option[String],
    dataDir: String,
    queries: Option[String]
  ) extends TpcDsBase(dataDir)
    with Workload {
  import TpcDsWorkload._

  private def runQuery(queryNum: Int)(implicit spark: SparkSession): Seq[TpcDsQueryStats] = {
    val queryName = s"hdfs:///$tpcdsQueriesDir/query${f"$queryNum%02d"}.sql"
    val (_, content) = spark.sparkContext.wholeTextFiles(queryName).collect()(0)
    val queries = content.split("\n").filterNot(_.startsWith("--")).mkString(" ").split(";")
    if (queries.isEmpty) throw new Exception(s"No queries to run for $queryName - ")
    log.error(s"Running TPC-DS Query $queryName")

    queries.map { query =>
      val (dur, result) = time(spark.sql(query).collect)
      TpcDsQueryStats(queryName, dur, result.length)
    }
  }

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    implicit val explicitlyDeclaredImplicitSpark: SparkSession = spark
    spark.sql(s"USE $tpcdsDatabaseName")
    val queryStats = mkQueries(queries).map { queryNum => (queryNum, runQuery(queryNum)) }
    spark.createDataFrame(spark.sparkContext.parallelize(queryStats))
  }
}
