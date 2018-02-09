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

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService => ExSvc, Future}
import scala.io.Source.fromInputStream
import scala.util.Try

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.{syncCopy, tables}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.SaveModes
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}

object TpcDsDataGen extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val name = "tpcdsdatagen"

  private val maxThreads = 20
  private val rngSeedDefault = 100
  private val scaleDefault = 1

  private val PartitionedRgx = "([a-z_]+)_[0-9]+_[0-9]+.dat".r
  private val NonPartitionedRgx = "([a-z_]+).dat".r

  def apply(m: Map[String, Any]): Workload = TpcDsDataGen(
    optionallyGet(m, "input"),
    optionallyGet(m, "output"),
    getOrDefault[String](m, "save-mode", SaveModes.error),
    getOrDefault[String](m, "dbname", "tpcds"),
    getOrDefault[String](m, "warehouse", "spark-warehouse"),
    getOrDefault[Boolean](m, "clean", false),
    TableOptions(m),
    getOrThrowT[String](m, "tpcds-kit-dir"),
    getOrDefault[Int](m, "tpcds-scale", scaleDefault),
    getOrDefault[Int](m, "tpcds-rngseed", rngSeedDefault),
    getOrDefault[String](m, "tpcds-output", "tpcds-data"),
    getOrThrowT[String](m, "tpcds-data-output")
  )
}

case class TpcDsDataGen(
    input: Option[String],
    output: Option[String],
    saveMode: String,
    dbName: String,
    warehouse: String,
    clean: Boolean,
    tableOptions: Seq[TableOptions],
    tpcDsKitDir: String,
    tpcDsScale: Int,
    tpcDsRngSeed: Int,
    tpcDsOutput: String,
    tpcDsDataOutput: String
  ) extends Workload {
  import TpcDsDataGen._

  private[tpcds] def asyncCopy(file: File, tableName: String)(implicit ec: ExSvc) = Future {
    syncCopy(file, s"$tpcDsDataOutput/$tableName")
  }.recover { case e: Throwable => log.error(s"FileUtil.copy failed on ${file.getName}", e); false }

  protected[tpcds] def createDatabase()(implicit spark: SparkSession): Unit = {
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

  private[tpcds] def mkPartitionStatement(tableName: String): String = {
    tableOptions.find(_.name == tableName) match {
      case Some(to) if to.partitionColumns.nonEmpty => s"PARTITIONED BY(${to.partitionColumns.mkString(", ")})"
      case _ => ""
    }
  }

  /**
   * Function to create a table in spark. It reads the DDL script for each of the
   * tpc-ds table and executes it on Spark.
   */
  protected[tpcds] def createTable(tableName: String)(implicit spark: SparkSession): Unit = {
    log.error(s"Creating table $tableName...")
    val rs = getClass.getResourceAsStream(s"/tpcds/ddl/$tableName.sql")
    val sqlStmts = fromInputStream(rs)
      .mkString
      .stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", tpcDsDataOutput)
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .replace("${PARTITIONEDBY_STATEMENT}", mkPartitionStatement(tableName))
      .split(";")
    sqlStmts.map { sqlStmt =>
      log.error(sqlStmt)
      spark.sql(sqlStmt)
    }
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
      val futures = files.map { f => asyncCopy(f, extractTableName(f.getName)) }
      files.map(_.getName).zip(waitForFutures(futures)).map(TpcDsTableGenResults(_))
    } else Seq.empty
  }

  private[tpcds] def mkCmd(topt: TableOptions, child: Int, outputDir: String) = {
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
        "-child", s"$child",
        "-parallel", s"$partitions"
      )
    }
  }

  private[tpcds] val outputPath: String = {
    if (tpcDsOutput.nonEmpty && tpcDsOutput.endsWith("/")) tpcDsOutput
    else if (tpcDsOutput.nonEmpty) tpcDsOutput + "/"
    else tpcDsOutput
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

  private[tpcds] def validateResults(results: Seq[TpcDsTableGenResults])(implicit spark: SparkSession): Unit = {
    results.foreach { i => log.error(i.toString) }
    if (results.isEmpty) {
      throw new RuntimeException("Data generation failed for tpcds. Check the executor logs for details.")
    } else {
      val tablesPresentInOutput = tables.flatMap { table =>
        results
          .find { r => r.table.startsWith(table) && r.res }
          .orElse { log.error(s"Failed to find $table in output"); None }
      }
      if (tablesPresentInOutput.length != tables.length) {
        throw new RuntimeException(
          "Not all tables are present in the output. Check the executor logs for more details."
        )
      }
    }
  }

  private def genData(topt: TableOptions, child: Int): Seq[TpcDsTableGenResults] = {
    val outputDir = s"$outputPath${topt.name}$child"
    try {
      val f = new File(outputDir)
      if (!f.exists) f.mkdirs
      log.debug(s"Outputting data to ${f.getAbsolutePath}")
      if (runCmd(mkCmd(topt, child, outputDir))) copyToHdfs(f)
      else Seq.empty[TpcDsTableGenResults]
    } catch { case e: Throwable =>
      log.error(s"Failed to handle partition #$child", e)
      Seq.empty[TpcDsTableGenResults]
    } finally {
      deleteLocalDir(outputDir)
    }
  }

  private def genData(topt: TableOptions)
      (implicit spark: SparkSession, exSvc: ExSvc): Future[RDD[Seq[TpcDsTableGenResults]]] = Future {
    val numPartitions = topt.partitions.getOrElse(1)
    spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map(genData(topt, _))
  }

  private[tpcds] def genDataWithTiming(topts: Seq[TableOptions])
      (implicit spark: SparkSession, exSvc: ExSvc) = {
    topts.filterNot(_.skipgen.getOrElse(false)).map { topt =>
      val (t, res) = time(genData(topt))
      (topt.name, t, res)
    }
  }

  private def genData()(implicit spark: SparkSession, exSvc: ExSvc): Seq[TpcDsTableGenStats] = {
    try {
      val seqOfRdds = genDataWithTiming(tableOptions)
      val rdds = waitForFutures(seqOfRdds.map(_._3))
      validateResults(rdds.flatMap(_.collect).flatten)
      seqOfRdds.map(TpcDsTableGenStats(_))
    } catch { case e: Throwable =>
      log.error("Failed to generate data for TPC-DS", e)
      Seq.empty[TpcDsTableGenStats]
    }
  }

  private def addRowCountsToStats(
      stats: Seq[TpcDsTableGenStats],
      rowCounts: Seq[(String, Long)]) = stats.map { r =>
    rowCounts.find(_._1 == r.table) match {
      case Some(rc) => r.copy(rowCount = rc._2)
      case _ => r
    }
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    implicit val explicitlyDeclaredImplicitSpark = spark
    implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(tableOptions.length))
    val stats = genData()
    createDatabase()
    // TODO: optimization possible here.
    // create the tables (convert from CSV to parquet) right after each table has been created
    tables.foreach(createTable)
    val rowCounts = tables.map(validateRowCount)
    val statsWithRowCounts = addRowCountsToStats(stats, rowCounts)
    spark.sparkContext.parallelize(statsWithRowCounts).toDF
  }
}
