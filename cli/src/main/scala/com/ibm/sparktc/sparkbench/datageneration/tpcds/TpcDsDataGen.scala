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
import java.util.concurrent.Executors.newFixedThreadPool

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService => ExSvc, Future}
import scala.sys.process._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}

// TODO: resolve pathing issues in a more robust manner.
// inserting hdfs was temporary just to get going

object TpcDsDataGen extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val name = "tpcdsdatagen"

  private def mkKitDir(m: Map[String, Any]): Option[String] = m.get("tpcds-kit-dir") match {
    case Some(d: String) => Some(d)
    case _ => None
  }

  def apply(m: Map[String, Any]): Workload = TpcDsDataGen(
    optionallyGet(m, "input"),
    optionallyGet(m, "output"),
    getOrDefault[String](m, "repo", "https://github.com/SparkTC/tpcds-journey.git"),
    getOrDefault[String](m, "datadir", "tpcds-data"),
    getOrDefault[String](m, "warehouse", "spark-warehouse"),
    getOrDefault[Boolean](m, "clean", false),
    mkKitDir(m),
    getOrDefault[Int](m, "tpcds-scale", 1),
    getOrDefault[Int](m, "tpcds-partitions", 1)
  )
}

case class TpcDsDataGen(
    input: Option[String],
    output: Option[String],
    repo: String,
    dataDir: String,
    warehouse: String,
    clean: Boolean,
    tpcDsKitDir: Option[String],
    tpcDsScale: Int,
    tpcDsPartitions: Int
  ) extends TpcDsBase(dataDir)
    with Workload {
  import TpcDsDataGen._

  protected def createDatabase(implicit spark: SparkSession): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $tpcdsDatabaseName CASCADE")
    spark.sql(s"CREATE DATABASE $tpcdsDatabaseName")
    spark.sql(s"USE $tpcdsDatabaseName")
  }

  private def deleteDir(dirName: String): Unit = Try {
    log.debug(s"Deleting directory: $dirName")
    val f = new File(dirName)
    if (f.exists) {
      f.listFiles.foreach(_.delete)
      f.delete
    }
  }.recover { case e: Throwable => log.error(s"Failed to cleanup $dirName", e) }

  private def deleteTableFromDisk(tableName: String): Unit =
    deleteDir(s"$warehouse/${tpcdsDatabaseName.toLowerCase}.db/$tableName")

  /**
   * Function to create a table in spark. It reads the DDL script for each of the
   * tpc-ds table and executes it on Spark.
   */
  protected def createTable(tableName: String)(implicit spark: SparkSession): Unit = {
    log.error(s"Creating table $tableName ..")
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    deleteTableFromDisk(tableName)
    val (_, content) = spark.sparkContext.wholeTextFiles(s"hdfs:///$tpcdsDdlDir/$tableName.sql").collect()(0)

    // Remove the replace for the .dat once it is fixed in the github repo
    val sqlStmts = content.stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", s"hdfs:///$tpcdsGenDataDir")
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .split(";")
    sqlStmts.map(spark.sql)
  }

  private def cleanup(): Unit = {
    deleteDir(dataDir)
    // don't delete the hdfs directory bc probably won't have permissions to recreate it
    s"hdfs dfs -rm -r -f hdfs:///$dataDir/*".!
  }

  private def retrieveRepo(): Unit = {
    val dirExists = if (clean) {
      cleanup()
      1
    } else s"hdfs dfs -test -d hdfs:///$dataDir".!
    if (dirExists != 0) {
      if (!new File(dataDir).exists()) s"git clone --progress $repo $dataDir".!
      s"hdfs dfs -copyFromLocal $dataDir hdfs:///".!
    }
  }

  private def genFromJourney(implicit spark: SparkSession): Unit = {
    "git --version".!
    retrieveRepo()
    createDatabase
    forEachTable(tables, createTable)
    spark.sql("show tables").collect.foreach(s => log.error(s.mkString(" | ")))
  }

  private def asyncCopy(file: File)(implicit bvRootDir: Broadcast[String], ec: ExSvc): Future[Boolean] = {
    val conf = new Configuration()
    val dstDir = new Path(s"hdfs:///${bvRootDir.value}", file.getName)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    Future(FileUtil.copy(file, dstFs, dstDir, false, conf)).recover {
      case e: Throwable => log.error(s"FileUtil.copy failed on ${file.getName}", e); false
    }
  }

  private def waitForFutures(futures: Seq[Future[Boolean]])(implicit ec: ExSvc): Seq[Boolean] = {
    try Await.result(Future.sequence(futures), Duration.Inf)
    finally ec.shutdown()
  }

  private def copyToHdfs(f: File)(implicit bvRootDir: Broadcast[String]): Seq[(String, Boolean)] = {
    val files = f.listFiles
    if (files.nonEmpty) {
      implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(files.length))
      val futures = files.map(asyncCopy)
      files.map(_.getName).zip(waitForFutures(futures))
    } else Seq.empty
  }

  // runCmd runs the dsdgen command and checks the stderr to see if an error occurred
  // normally the return value of the command would indicate a failure but that's not how tpc-ds rolls
  private def runCmd(cmd: Seq[String]): Boolean = Try {
    log.debug(s"runCmd: ${cmd.mkString(" ")}")
    val (stdout, stderr) = (new StringBuilder, new StringBuilder)
    cmd ! ProcessLogger(stdout.append(_), stderr.append(_)) match {
      case ret if ret == 0 && !stderr.toString().contains("ERROR") =>
        log.debug(s"stdout: $stdout\nstderr: $stderr")
      case ret =>
        throw new RuntimeException(s"dsdgen failed: $ret\n${stderr.toString}")
    }
  }.recover { case e: Throwable => log.error(s"Failed to run command ${cmd.mkString(" ")}", e) }.isSuccess

  private def fixupDataDirPath: String = dataDir match {
    case d if d.nonEmpty && d.endsWith("/") => d
    case d if d.nonEmpty => d + "/"
    case d => d
  }

  private def genData(kitDir: String)(implicit spark: SparkSession): Unit = {
    val bvScale = spark.sparkContext.broadcast(tpcDsScale)
    val bvPartitions = spark.sparkContext.broadcast(tpcDsPartitions)
    val bvKitDir = spark.sparkContext.broadcast(kitDir)
    val bvDataDir = spark.sparkContext.broadcast(fixupDataDirPath)
    implicit val bvRootDir = spark.sparkContext.broadcast(tpcdsRootDir)
    spark.sparkContext.parallelize(1 to tpcDsPartitions, tpcDsPartitions).map { chile =>
      val outputDir = s"${bvDataDir.value}tpcds$chile"
      try {
        val f = new File(outputDir)
        if (!f.exists()) f.mkdirs
        log.error(s"Outputting data to ${f.getAbsolutePath}")

        val cmd = Seq(
          s"${bvKitDir.value}/tools/dsdgen",
          "-sc", s"${bvScale.value}",
          "-child", s"$chile",
          "-parallel", s"${bvPartitions.value}",
          "-distributions", s"${bvKitDir.value}/tools/tpcds.idx",
          "-dir", outputDir
        )
        if (runCmd(cmd)) copyToHdfs(f)
      } catch { case e: Throwable => log.error(s"Failed to handle partition #$chile", e) }
      finally deleteDir(outputDir)
    }.collect.foreach { i => log.error(i.toString) }
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    implicit val impSpark: SparkSession = spark
    tpcDsKitDir match {
      case Some(kitDir) => genData(kitDir)
      case _ => genFromJourney
    }
    spark.emptyDataFrame
  }
}
