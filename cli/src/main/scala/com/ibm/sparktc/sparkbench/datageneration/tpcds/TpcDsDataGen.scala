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
import scala.concurrent.{Await, ExecutionContext, Future, ExecutionContextExecutorService => ExSvc}
import scala.sys.process._
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.rdd.RDD

object TpcDsDataGen extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val name = "tpcdsdatagen"

  def apply(m: Map[String, Any]): Workload = TpcDsDataGen(
    optionallyGet(m, "input"),
    optionallyGet(m, "output"),
    getOrDefault[String](m, "repo", "https://github.com/SparkTC/tpcds-journey.git"),
    getOrDefault[String](m, "journeydir", "tpcds-journey"),
    getOrDefault[String](m, "dbname", "tpcds"),
    getOrDefault[String](m, "warehouse", "spark-warehouse"),
    getOrDefault[Boolean](m, "clean", false),
    getOrDefault[String](m, "fsprefix", "hdfs:///"),
    TableOptions(m),
    optionallyGet(m, "tpcds-kit-dir"),
    getOrDefault[Int](m, "tpcds-scale", 1),
    // scalastyle:off magic.number
    getOrDefault[Int](m, "tpcds-rngseed", 100)
    // scalastyle:on magic.number
  )
}

case class TpcDsDataGen(
    input: Option[String],
    output: Option[String],
    repo: String,
    journeyDir: String,
    dbName: String,
    warehouse: String,
    clean: Boolean,
    fsPrefix: String,
    tableOptions: Option[Seq[TableOptions]],
    tpcDsKitDir: Option[String],
    tpcDsScale: Int,
    tpcDsRngSeed: Int
  ) extends TpcDsBase(journeyDir, dbName)
    with Workload {
  import TpcDsDataGen._

  private val conf = new Configuration

  protected def createDatabase(implicit spark: SparkSession): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $tpcdsDatabaseName CASCADE")
    spark.sql(s"CREATE DATABASE $tpcdsDatabaseName")
    spark.sql(s"USE $tpcdsDatabaseName")
  }

  private def deleteLocalDir(dirName: String): Unit = Try {
    log.debug(s"Deleting local directory: $dirName")
    val f = new File(dirName)
    if (f.exists) {
      f.listFiles.foreach(_.delete)
      f.delete
    }
  }.recover { case e: Throwable => log.error(s"Failed to cleanup $dirName", e) }

  private def deleteTableFromDisk(tableName: String): Unit =
    deleteLocalDir(s"$warehouse/${tpcdsDatabaseName.toLowerCase}.db/$tableName")

  private def getOutputDataDir = tpcDsKitDir match {
    case Some(_) => output.getOrElse(s"${fsPrefix}tpcds-data")
    case _ => s"$fsPrefix$tpcdsGenDataDir"
  }

  /**
   * Function to create a table in spark. It reads the DDL script for each of the
   * tpc-ds table and executes it on Spark.
   */
  protected def createTable(tableName: String)(implicit spark: SparkSession): Unit = {
    log.error(s"Creating table $tableName...")
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    deleteTableFromDisk(tableName)
    val (_, content) = spark.sparkContext.wholeTextFiles(s"$fsPrefix$tpcdsDdlDir/$tableName.sql").collect()(0)

    // Remove the replace for the .dat once it is fixed in the github repo
    val sqlStmts = content.stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", s"$getOutputDataDir")
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .split(";")
    sqlStmts.map(spark.sql)
  }

  private def cleanup(): Unit = {
    deleteLocalDir(journeyDir)
    val dstDir = new Path(s"$fsPrefix$journeyDir")
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    dstFs.delete(dstDir, true)
  }

  private def retrieveRepo(): Unit = {
    val dirExists = if (clean) {
      cleanup()
      false
    } else {
      val dstDir = new Path(s"$fsPrefix$journeyDir")
      val dstFs = FileSystem.get(dstDir.toUri, conf)
      dstFs.isDirectory(dstDir)
    }
    if (!dirExists) {
      if (!new File(journeyDir).exists()) s"git clone --progress $repo $journeyDir".!
      val file = new File(journeyDir)
      file.listFiles.foreach { f =>
        val dstDir = new Path(s"$fsPrefix$journeyDir", f.getName)
        val dstFs = FileSystem.get(dstDir.toUri, conf)
        FileUtil.copy(f, dstFs, dstDir, false, conf)
      }
    }
  }

  private def genFromJourney(implicit spark: SparkSession): Unit = {
    "git --version".!
    retrieveRepo()
  }

  private def asyncCopy(file: File)(implicit ec: ExSvc): Future[Boolean] = Future {
    val dstDir = new Path(getOutputDataDir, file.getName)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    FileUtil.copy(file, dstFs, dstDir, false, conf)
  }.recover { case e: Throwable => log.error(s"FileUtil.copy failed on ${file.getName}", e); false }

  private def waitForFutures(futures: Seq[Future[Boolean]])(implicit ec: ExSvc): Seq[Boolean] = {
    try Await.result(Future.sequence(futures), Duration.Inf)
    finally ec.shutdown()
  }

  private def copyToHdfs(f: File): Seq[(String, Boolean)] = {
    val files = f.listFiles
    if (files.nonEmpty) {
      implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(files.length))
      val futures = files.map(asyncCopy)
      files.map(_.getName).zip(waitForFutures(futures))
    } else Seq.empty
  }

  private def mkCmd(kitDir: String, chile: Int, outputDir: String, topt: TableOptions) = {
    val cmd = Seq(
      s"$kitDir/tools/dsdgen",
      "-sc", s"$tpcDsScale",
      "-distributions", s"$kitDir/tools/tpcds.idx",
      "-rngseed", s"$tpcDsRngSeed",
      "-table", topt.name,
      "-dir", outputDir
    )
    topt.partitions.foldLeft(cmd) { case (acc, partitions) =>
      acc ++ Seq(
        "-child", s"$chile",
        "-parallel", s"$partitions"
      )
    }
  }

  // runCmd runs the dsdgen command and checks the stderr to see if an error occurred
  // normally the return value of the command would indicate a failure but that's not how tpc-ds rolls
  private def runCmd(cmd: Seq[String]): Boolean = Try {
    log.debug(s"runCmd: ${cmd.mkString(" ")}")
    val (stdout, stderr) = (new StringBuilder, new StringBuilder)
    cmd ! ProcessLogger(stdout.append(_), stderr.append(_)) match {
      case ret if ret == 0 && !stderr.toString.contains("ERROR") =>
        log.debug(s"stdout: $stdout\nstderr: $stderr")
      case ret =>
        throw new RuntimeException(s"dsdgen failed: $ret\n\nstderr\n${stderr.toString}")
    }
  }.recover { case e: Throwable => log.error(s"Failed to run command ${cmd.mkString(" ")}", e) }.isSuccess

  private def fixupOutputDirPath: String = output match {
    case Some(d) if d.nonEmpty && d.endsWith("/") => d
    case Some(d) if d.nonEmpty => d + "/"
    case Some(d) => d
    case _ => ""
  }

  private def validateResults(results: Seq[(String, Boolean)]): Unit = {
    results.foreach { i => log.error(i.toString) }
    if (results.isEmpty) throw new RuntimeException("Data generation failed for tpcds. Check the executor logs for details.")
    else {
      val tablesPresentInOutput = TpcDsBase.tables.flatMap { table =>
        results.find { case (fn, res) => fn.startsWith(table) && res } match {
          case None => log.error(s"Failed to find $table in output"); None
          case r => r
        }
      }
      if (tablesPresentInOutput.length != TpcDsBase.tables.length)
        throw new RuntimeException("Not all tables are present in the output. Check the executor logs for more details.")
    }
  }

  private def genData(kitDir: String, topt: TableOptions)(implicit spark: SparkSession): RDD[Seq[(String, Boolean)]] = {
    val numPartitions = topt.partitions.getOrElse(1)
    spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map { chile =>
      val outputDir = s"$fixupOutputDirPath${topt.name}"
      try {
        val f = new File(outputDir)
        if (!f.exists) f.mkdirs
        log.debug(s"Outputting data to ${f.getAbsolutePath}")
        if (runCmd(mkCmd(kitDir, chile, outputDir, topt))) copyToHdfs(f)
        else Seq.empty[(String, Boolean)]
      } catch { case e: Throwable => log.error(s"Failed to handle partition #$chile", e); Seq.empty[(String, Boolean)] }
      finally deleteLocalDir(outputDir)
    }
  }

  private def genData(kitDir: String)(implicit spark: SparkSession): Unit = {
    val seqOfRdds = tableOptions match {
      case Some(topts) => topts.map(genData(kitDir, _))
      case _ => throw new Exception( s"""No tables specified for generation.
        | $name workloads must have a table-options configuration string""".stripMargin)
    }
    val rdd = seqOfRdds.flatMap(_.collect).flatten
    validateResults(rdd)
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    implicit val explicitlyDeclaredImplicitSpark = spark
    genFromJourney
    tpcDsKitDir.foreach(genData)
    createDatabase
    forEachTable(tables, createTable)
    spark.sql("show tables")
  }
}
