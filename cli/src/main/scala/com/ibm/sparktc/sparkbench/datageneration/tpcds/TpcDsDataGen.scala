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
 * Much of the credit for this workload goes to the work by Xin Wu and his team while making their TPC-DS
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}

object TpcDsDataGen extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val name = "tpcdsdatagen"

  private val maxThreads = 20
  private val rngSeedDefault = 100
  private val scaleDefault = 1

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
    getOrDefault[Int](m, "tpcds-scale", scaleDefault),
    getOrDefault[Int](m, "tpcds-rngseed", rngSeedDefault)
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
    tableOptions: Seq[TableOptions],
    tpcDsKitDir: Option[String],
    tpcDsScale: Int,
    tpcDsRngSeed: Int
  ) extends TpcDsBase(journeyDir, dbName)
    with Workload {
  import TpcDsDataGen._

  private[tpcds] def syncCopy(file: File, outputDir: String)(implicit conf: Configuration) = {
    val dstDir = new Path(outputDir, file.getName)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    FileUtil.copy(file, dstFs, dstDir, false, conf)
  }

  private[tpcds] def asyncCopy(file: File)(implicit ec: ExSvc, conf: Configuration): Future[Boolean] = Future {
    syncCopy(file, getOutputDataDir)
  }.recover { case e: Throwable => log.error(s"FileUtil.copy failed on ${file.getName}", e); false }

  protected[tpcds] def createDatabase(implicit spark: SparkSession): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $tpcdsDatabaseName CASCADE")
    spark.sql(s"CREATE DATABASE $tpcdsDatabaseName")
    spark.sql(s"USE $tpcdsDatabaseName")
  }

  private[tpcds] def deleteLocalDir(dirName: String): Unit = Try {
    log.debug(s"Deleting local directory: $dirName")
    val f = new File(dirName)
    if (f.isDirectory) {
      f.listFiles.foreach {
        case ff if ff.isDirectory => deleteLocalDir(ff.getAbsolutePath)
        case ff => ff.delete
      }
      f.delete
    }
  }.recover { case e: Throwable => log.error(s"Failed to cleanup $dirName", e) }

  private def deleteTableFromDisk(tableName: String): Unit =
    deleteLocalDir(s"$warehouse/${tpcdsDatabaseName.toLowerCase}.db/$tableName")

  private[tpcds] def getOutputDataDir = s"$fsPrefix${
    tpcDsKitDir match {
      case Some(_) => output.getOrElse("tpcds-data")
      case _ => tpcdsGenDataDir
    }
  }"

  /**
   * Function to create a table in spark. It reads the DDL script for each of the
   * tpc-ds table and executes it on Spark.
   */
  protected[tpcds] def createTable(tableName: String)(implicit spark: SparkSession): Unit = {
    log.error(s"Creating table $tableName...")
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    deleteTableFromDisk(tableName)
    val (_, content) = spark.sparkContext.wholeTextFiles(s"$fsPrefix$tpcdsDdlDir/$tableName.sql").collect()(0)

    // Remove the replace for the .dat once it is fixed in the github repo
    val sqlStmts = content.stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", getOutputDataDir)
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .split(";")
    sqlStmts.map(spark.sql)
  }

  private[tpcds] def cleanupJourney(implicit conf: Configuration): Unit = if (clean) {
    val dstDir = new Path(s"$fsPrefix$journeyDir")
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    deleteLocalDir(journeyDir)
    dstFs.delete(dstDir, true)
  }

  private[tpcds] def doesJourneyExist(implicit conf: Configuration): Boolean = {
    val dstDir = new Path(s"$fsPrefix$journeyDir")
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    dstFs.isDirectory(dstDir)
  }

  private[tpcds] def retrieveRepo(): Unit = {
    implicit val conf = new Configuration
    cleanupJourney
    val journeyExists = doesJourneyExist
    if (!journeyExists) {
      if (!new File(journeyDir).exists()) s"git clone --progress $repo $journeyDir".!
      val file = new File(journeyDir)
      file.listFiles.foreach(syncCopy(_, s"$fsPrefix$journeyDir"))
    }
  }

  private def genFromJourney(implicit spark: SparkSession): Unit = {
    "git --version".!
    retrieveRepo()
  }

  private[tpcds] def waitForFutures[T](futures: Seq[Future[T]])(implicit ec: ExSvc): Seq[T] = {
    try Await.result(Future.sequence(futures), Duration.Inf)
    finally ec.shutdown()
  }

  private def copyToHdfs(f: File): Seq[TpcDsTableGenResults] = {
    val files = f.listFiles
    if (files.nonEmpty) {
      implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(math.min(files.length, maxThreads)))
      implicit val conf = new Configuration
      val futures = files.map(asyncCopy)
      files.map(_.getName).zip(waitForFutures(futures)).map(TpcDsTableGenResults(_))
    } else Seq.empty
  }

  private[tpcds] def mkCmd(kitDir: String, topt: TableOptions, chile: Int, outputDir: String) = {
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
  private[tpcds] def runCmd(cmd: Seq[String]): Boolean = Try {
    log.debug(s"runCmd: ${cmd.mkString(" ")}")
    val (stdout, stderr) = (new StringBuilder, new StringBuilder)
    cmd ! ProcessLogger(stdout.append(_), stderr.append(_)) match {
      case ret if ret == 0 && !stderr.toString.contains("ERROR") =>
        log.debug(s"stdout: $stdout\nstderr: $stderr")
      case ret =>
        val msg = s"dsdgen failed: $ret\n\nstderr\n${stderr.toString}"
        log.error(msg)
        throw new RuntimeException(msg)
    }
  }.isSuccess

  private[tpcds] def fixupOutputDirPath: String = output match {
    case Some(d) if d.nonEmpty && d.endsWith("/") => d
    case Some(d) if d.nonEmpty => d + "/"
    case Some(d) => d
    case _ => ""
  }

  private[tpcds] def validateResults(results: Seq[TpcDsTableGenResults]): Unit = {
    results.foreach { i => log.error(i.toString) }
    if (results.isEmpty) throw new RuntimeException("Data generation failed for tpcds. Check the executor logs for details.")
    else {
      val tablesPresentInOutput = TpcDsBase.tables.flatMap { table =>
        results
          .find { r => r.table.startsWith(table) && r.res }
          .orElse { log.error(s"Failed to find $table in output"); None }
      }
      if (tablesPresentInOutput.length != TpcDsBase.tables.length)
        throw new RuntimeException("Not all tables are present in the output. Check the executor logs for more details.")
    }
  }

  private def genData(kitDir: String, topt: TableOptions, chile: Int) = {
    val outputDir = s"$fixupOutputDirPath${topt.name}$chile"
    try {
      val f = new File(outputDir)
      if (!f.exists) f.mkdirs
      log.debug(s"Outputting data to ${f.getAbsolutePath}")
      if (runCmd(mkCmd(kitDir, topt, chile, outputDir))) copyToHdfs(f)
      else Seq.empty[TpcDsTableGenResults]
    } catch { case e: Throwable => log.error(s"Failed to handle partition #$chile", e); Seq.empty[TpcDsTableGenResults] }
    finally deleteLocalDir(outputDir)
  }

  private def genData(kitDir: String, topt: TableOptions)(implicit spark: SparkSession, exSvc: ExSvc): Future[RDD[Seq[TpcDsTableGenResults]]] = Future {
    val numPartitions = topt.partitions.getOrElse(1)
    spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map(genData(kitDir, topt, _))
  }

  private[tpcds] def genDataWithTiming(kitDir: String, topts: Seq[TableOptions])(implicit spark: SparkSession, exSvc: ExSvc) = {
    topts.map { topt =>
      val (t, res) = time(genData(kitDir, topt))
      (topt.name, t, res)
    }
  }

  private def genData(kitDir: String)(implicit spark: SparkSession, exSvc: ExSvc): Seq[TpcDsTableGenStats] = {
    val seqOfRdds = genDataWithTiming(kitDir, tableOptions)
    val rdds = waitForFutures(seqOfRdds.map(_._3))
    validateResults(rdds.flatMap(_.collect).flatten)
    seqOfRdds.map(TpcDsTableGenStats(_))
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    implicit val explicitlyDeclaredImplicitSpark = spark
    import spark.sqlContext.implicits._
    genFromJourney
    val res = tpcDsKitDir.map { kitDir =>
      implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(tableOptions.length))
      genData(kitDir)
    }
    createDatabase
    forEachTable(tables, createTable)
    // TODO: make this output consistent. it's just weird bc no data generation occurs if the kit is
    // not defined (i.e. it simply uses the dataset provided in the journey).
    res match {
      case Some(r) => spark.sparkContext.parallelize(r).toDF
      case _ => spark.sql("show tables")
    }
  }
}
