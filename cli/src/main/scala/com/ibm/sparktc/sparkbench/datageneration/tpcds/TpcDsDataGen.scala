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
import scala.io.Source.fromInputStream
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.{NonPartitionedRgx, PartitionedRgx, tables}
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
    getOrDefault[String](m, "dbname", "tpcds"),
    getOrDefault[String](m, "warehouse", "spark-warehouse"),
    getOrDefault[Boolean](m, "clean", false),
    getOrDefault[String](m, "fsprefix", "hdfs:///"),
    TableOptions(m),
    getOrThrowT[String](m, "tpcds-kit-dir"),
    getOrDefault[Int](m, "tpcds-scale", scaleDefault),
    getOrDefault[Int](m, "tpcds-rngseed", rngSeedDefault)
  )
}

case class TpcDsDataGen(
    input: Option[String],
    output: Option[String],
    dbName: String,
    warehouse: String,
    clean: Boolean,
    fsPrefix: String,
    tableOptions: Seq[TableOptions],
    tpcDsKitDir: String,
    tpcDsScale: Int,
    tpcDsRngSeed: Int
  ) extends Workload {
  import TpcDsDataGen._

  private[tpcds] def syncCopy(file: File, outputDir: String)(implicit conf: Configuration) = {
    val dstDir = if (file.isDirectory) new Path(outputDir, file.getName) else new Path(outputDir)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    // if just copying a file, make sure the directory exists
    // if copying a directory, don't do this
    if (!file.isDirectory && !dstFs.isDirectory(dstDir)) dstFs.mkdirs(dstDir)
    FileUtil.copy(file, dstFs, dstDir, false, conf)
  }

  private[tpcds] def asyncCopy(file: File, tableName: String)(implicit ec: ExSvc, conf: Configuration): Future[Boolean] = Future {
    syncCopy(file, s"$getOutputDataDir/$tableName")
  }.recover { case e: Throwable => log.error(s"FileUtil.copy failed on ${file.getName}", e); false }

  protected[tpcds] def createDatabase(implicit spark: SparkSession): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    spark.sql(s"CREATE DATABASE $dbName")
    spark.sql(s"USE $dbName")
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

  private[tpcds] def deleteTableFromDisk(tableName: String): Unit =
    deleteLocalDir(s"$warehouse/${dbName.toLowerCase}.db/$tableName")

  private[tpcds] def getOutputDataDir = s"$fsPrefix${output.getOrElse("tpcds-data")}"

  /**
   * Function to create a table in spark. It reads the DDL script for each of the
   * tpc-ds table and executes it on Spark.
   */
  protected[tpcds] def createTable(tableName: String)(implicit spark: SparkSession): Unit = {
    log.error(s"Creating table $tableName...")
//    spark.sql(s"DROP TABLE IF EXISTS $tableName")
//    deleteTableFromDisk(tableName)

    val rs = getClass.getResourceAsStream(s"/tpcds/ddl/$tableName.sql")
    val sqlStmts = fromInputStream(rs)
      .mkString
      .stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", getOutputDataDir)
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .split(";")
    sqlStmts.map(spark.sql)
  }

  private[tpcds] def waitForFutures[T](futures: Seq[Future[T]])(implicit ec: ExSvc): Seq[T] = {
    try Await.result(Future.sequence(futures), Duration.Inf)
    finally ec.shutdown()
  }

  /**
    * Since dsdgen can create multiple tables (e.g. child tables) per call, can't simply
    * depend on the table name passed to the command. Instead, the table name is determined
    * by the file name.
    */
  private def extractTableName(fn: String): String = fn match {
    case PartitionedRgx(tn) => tn
    case NonPartitionedRgx(tn) => tn
    case _ => throw new RuntimeException(s"No regex patterns matched $fn")
  }

  private def copyToHdfs(f: File): Seq[TpcDsTableGenResults] = {
    val files = f.listFiles
    if (files.nonEmpty) {
      implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(math.min(files.length, maxThreads)))
      implicit val conf = new Configuration
      val futures = files.map { f => asyncCopy(f, extractTableName(f.getName)) }
      files.map(_.getName).zip(waitForFutures(futures)).map(TpcDsTableGenResults(_))
    } else Seq.empty
  }

  private[tpcds] def mkCmd(topt: TableOptions, chile: Int, outputDir: String) = {
    val cmd = Seq(
      s"$tpcDsKitDir/tools/dsdgen",
      "-sc", s"$tpcDsScale",
      "-distributions", s"$tpcDsKitDir/tools/tpcds.idx",
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

  private[tpcds] def fixupOutputDirPath: String = output match {
    case Some(d) if d.nonEmpty && d.endsWith("/") => d
    case Some(d) if d.nonEmpty => d + "/"
    case Some(d) => d
    case _ => ""
  }

  private[tpcds] def validateRowCount(table: String)(implicit spark: SparkSession): (String, Long) = {
    val expectedRowCount = TableRowCounts.getCount(table, tpcDsScale)
    val res = spark.sql(s"select count(*) from $table").collect
    if (res.isEmpty) throw new RuntimeException(s"Could not query $table's row count")
    else {
      val actualRowCount = res.head.getAs[Long](0)
      if (actualRowCount != expectedRowCount) {
        log.error(s"$table's row counts did not match expected. actual: $actualRowCount, expected: $expectedRowCount")
      }
      (table, actualRowCount)
    }
  }

  private[tpcds] def validateRowCounts(implicit spark: SparkSession): Seq[(String, Long)] = tables.map(validateRowCount)

  private[tpcds] def validateResults(results: Seq[TpcDsTableGenResults])(implicit spark: SparkSession): Unit = {
    results.foreach { i => log.error(i.toString) }
    if (results.isEmpty) throw new RuntimeException("Data generation failed for tpcds. Check the executor logs for details.")
    else {
      val tablesPresentInOutput = tables.flatMap { table =>
        results
          .find { r => r.table.startsWith(table) && r.res }
          .orElse { log.error(s"Failed to find $table in output"); None }
      }
      if (tablesPresentInOutput.length != tables.length) {
        throw new RuntimeException("Not all tables are present in the output. Check the executor logs for more details.")
      }
    }
  }

  private def genData(topt: TableOptions, chile: Int) = {
    val outputDir = s"$fixupOutputDirPath${topt.name}$chile"
    try {
      val f = new File(outputDir)
      if (!f.exists) f.mkdirs
      log.debug(s"Outputting data to ${f.getAbsolutePath}")
      if (runCmd(mkCmd(topt, chile, outputDir))) copyToHdfs(f)
      else Seq.empty[TpcDsTableGenResults]
    } catch { case e: Throwable => log.error(s"Failed to handle partition #$chile", e); Seq.empty[TpcDsTableGenResults] }
    finally deleteLocalDir(outputDir)
  }

  private def genData(topt: TableOptions)(implicit spark: SparkSession, exSvc: ExSvc): Future[RDD[Seq[TpcDsTableGenResults]]] = Future {
    val numPartitions = topt.partitions.getOrElse(1)
    spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map(genData(topt, _))
  }

  private[tpcds] def genDataWithTiming(topts: Seq[TableOptions])(implicit spark: SparkSession, exSvc: ExSvc) = {
    topts.map { topt =>
      val (t, res) = time(genData(topt))
      (topt.name, t, res)
    }
  }

  private def genData(implicit spark: SparkSession, exSvc: ExSvc): Seq[TpcDsTableGenStats] = {
    val seqOfRdds = genDataWithTiming(tableOptions)
    val rdds = waitForFutures(seqOfRdds.map(_._3))
    validateResults(rdds.flatMap(_.collect).flatten)
    seqOfRdds.map(TpcDsTableGenStats(_))
  }

  private def addRowCountsToStats(stats: Seq[TpcDsTableGenStats], rowCounts: Seq[(String, Long)]) = stats.map { r =>
    rowCounts.find(_._1 == r.table) match {
      case Some(rc) => r.copy(rowCount = rc._2)
      case _ => r
    }
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    implicit val explicitlyDeclaredImplicitSpark = spark
    implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(tableOptions.length))

    val stats = try genData
    catch { case e: Throwable => log.error("Failed to generate data for TPC-DS", e); Seq.empty[TpcDsTableGenStats] }
    finally ec.shutdown()

    createDatabase
    tables.foreach(createTable)
    val rowCounts = validateRowCounts
    val statsWithRowCounts = addRowCountsToStats(stats, rowCounts)
    spark.sparkContext.parallelize(statsWithRowCounts).toDF
  }
}
