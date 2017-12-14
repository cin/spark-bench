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

package com.ibm.sparktc.sparkbench.datageneration.tpcds

import java.io.File

import sys.process._
import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TpcDsDataGen extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val name = "tpcdsdatagen"
  def apply(m: Map[String, Any]): Workload = TpcDsDataGen(
    None,
    None,
    getOrDefault[String](m, "repo", "https://github.com/SparkTC/tpcds-journey.git"),
    getOrDefault[String](m, "datadir", "tpcds-data"),
    getOrDefault(m, "warehouse", "spark-warehouse")
  )
}

case class TpcDsDataGen(
    input: Option[String],
    output: Option[String],
    repo: String,
    dataDir: String,
    warehouse: String
  ) extends TpcDsBase(dataDir)
    with Workload {
  import TpcDsDataGen._

  protected def createDatabase(implicit spark: SparkSession): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $tpcdsDatabaseName CASCADE")
    spark.sql(s"CREATE DATABASE $tpcdsDatabaseName")
    spark.sql(s"USE $tpcdsDatabaseName")
  }

  private def deleteFile1(tableName: String): Unit = {
    s"rm -rf $warehouse/${tpcdsDatabaseName.toLowerCase}.db/$tableName/*".!
  }

  private def deleteFile2(tableName: String): Unit = {
    s"rm -rf $warehouse/${tpcdsDatabaseName.toLowerCase}.db/$tableName".!
  }

  /**
   * Function to create a table in spark. It reads the DDL script for each of the
   * tpc-ds table and executes it on Spark.
   */
  protected def createTable(tableName: String)(implicit spark: SparkSession): Unit = {
    log.error(s"Creating table $tableName ..")
//    spark.sql(s"DROP TABLE IF EXISTS $tableName")spark
    deleteFile1(tableName)
    deleteFile2(tableName)
    val (_, content) = spark.sparkContext.wholeTextFiles(s"hdfs:///$tpcdsDdlDir/$tableName.sql").collect()(0)

    // Remove the replace for the .dat once it is fixed in the github repo
    val sqlStmts = content.stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", s"hdfs:///$tpcdsGenDataDir")
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").split(";")
    sqlStmts.map(spark.sql)
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
//    "rm -rf $dataDir".!
    "git --version".!

    if (!new File(dataDir).exists()) s"git clone --progress $repo $dataDir".!
    val dirExists = s"hdfs dfs -test -d hdfs:///$dataDir".!
    if (dirExists != 0) s"hdfs dfs -copyFromLocal $dataDir hdfs:///$dataDir".!

    implicit val impSpark: SparkSession = spark
    createDatabase
    forEachTable(tables, createTable)
    spark.sql("show tables").collect.foreach(s => log.error(s.mkString(" | ")))
    spark.emptyDataFrame
  }
}
