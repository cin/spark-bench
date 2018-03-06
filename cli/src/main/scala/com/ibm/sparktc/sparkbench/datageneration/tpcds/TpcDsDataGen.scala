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

import scala.concurrent.{ExecutionContextExecutorService => ExSvc, Future}
import scala.io.Source.fromInputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase._
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
  private val ValidationRgx = "([a-z_]+).vld".r

  private val TPCDSISDAFT = """^\s*create\s+table\s+([a-zA-Z0-9_]+)\s*\(\s*(.*)$""".r
  private val TPCDSISSALES = """^\s*create\s+table\s+[a-z_]+(sales|returns)(.*)$""".r

  def apply(m: Map[String, Any]): Workload = {
    val dataOutput = getOrThrowT[String](m, "tpcds-data-output")
    TpcDsDataGen(
      optionallyGet(m, "input"),
      optionallyGet(m, "output"),
      getOrDefault[String](m, "save-mode", SaveModes.error),
      getOrDefault[String](m, "dbname", "tpcds"),
      getOrDefault[String](m, "warehouse", "spark-warehouse"),
      TableOptions(m),
      getOrThrowT[String](m, "tpcds-kit-dir"),
      getOrDefault[Int](m, "tpcds-scale", scaleDefault),
      getOrDefault[Int](m, "tpcds-rngseed", rngSeedDefault),
      getOrDefault[String](m, "tpcds-dsdgen-output", "tpcds-data"), // relative to the kitDir
      dataOutput,
      getOrDefault[Boolean](m, "tpcds-dsdgen-validate", false),
      getOrDefault[String](m, "tpcds-dsdgen-validation-output", "tpcds-validation-data"),
      getOrDefault[String](m, "tpcds-data-validation-output", s"$dataOutput-validation")
    )
  }
}

case class TpcDsDataGen(
    input: Option[String],
    output: Option[String],
    saveMode: String,
    dbName: String,
    warehouse: String,
    tableOptions: Seq[TableOptions],
    tpcDsKitDir: String,
    tpcDsScale: Int,
    tpcDsRngSeed: Int,
    tpcDsDsdgenOutput: String,
    tpcDsDataOutput: String,
    tpcDsValidate: Boolean,
    tpcDsDsdgenValidationOutput: String,
    tpcDsDataValidationOutput: String
  ) extends Workload {
  import TpcDsDataGen._

  private[tpcds] val dsdgenOutputPath: String = fixupPath(tpcDsDsdgenOutput)
  private[tpcds] val dsdgenValidationOutputPath: String = fixupPath(tpcDsDsdgenValidationOutput)
  private[tpcds] val validationDbName = s"${dbName}_validation"

  private[tpcds] def asyncCopy(file: File, tableName: String, validationPhase: Boolean)(implicit ec: ExSvc) = Future {
    syncCopy(file, if (validationPhase) s"$tpcDsDataValidationOutput/$tableName" else s"$tpcDsDataOutput/$tableName")
  }.recover { case e: Throwable => log.error(s"FileUtil.copy failed on ${file.getName}", e); false }

  protected[tpcds] def createDatabase()(implicit spark: SparkSession): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    spark.sql(s"CREATE DATABASE $dbName")
    spark.sql(s"USE $dbName")
  }

  private[tpcds] def mkPartitionStatement(tableName: String): String = {
    tableOptions.find(_.name == tableName) match {
      case Some(to) if to.partitionColumns.nonEmpty => s"PARTITIONED BY(${to.partitionColumns.mkString(", ")})"
      case _ => ""
    }
  }

  /**
    * For some reason, TPC-DS writes out an extra column when writing out validation rows.
    * The best thing about this is that the TPC-DS spec says nothing about this column or
    * what its expect output should be. In simple cases, it is the primary key. In multipart
    * keys, there doesn't seem to be a rhyme or reason to its value.
    *
    * This hack reuses the existing DDL but inserts a dummy column.
    */
  private[tpcds] def tpcdsIsDaft(qstr: String): String = qstr match {
    case TPCDSISSALES(sr, _) =>
      log.info(s"$sr table encountered.\n$qstr")
      qstr
    case TPCDSISDAFT(tablename, remainingDDL) => s"create table $tablename(dmy int, $remainingDDL"
    case _ =>
      log.error(s"Unable to match regex in validation DDL for create table string\n$qstr")
      qstr
  }

  private def pruneValidationStmts(sqlStmts: Seq[String], validationPhase: Boolean): Seq[String] = {
    if (validationPhase) Seq(sqlStmts.head, tpcdsIsDaft(sqlStmts(1)))
    else sqlStmts
  }

  /**
   * Function to create a table in spark. It reads the DDL script for each of the
   * tpc-ds table and executes it on Spark.
   */
  protected[tpcds] def createTable(tableName: String, validationPhase: Boolean)(implicit spark: SparkSession): Unit = {
    log.info(s"Creating table $tableName...")
    val rs = getClass.getResourceAsStream(s"/tpcds/ddl/$tableName.sql")
    val sqlStmts = fromInputStream(rs)
      .mkString
      .stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", if (validationPhase) tpcDsDataValidationOutput else tpcDsDataOutput)
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .replace("${PARTITIONEDBY_STATEMENT}", mkPartitionStatement(tableName))
      .split(";")
    pruneValidationStmts(sqlStmts, validationPhase).foreach { sqlStmt =>
      log.info(sqlStmt)
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
    case ValidationRgx(tn) => tn
    case _ => throw new RuntimeException(s"No regex patterns matched $fn")
  }

  private def copyToHdfs(f: File, validationPhase: Boolean): Seq[TpcDsTableGenResults] = {
    val files = f.listFiles
    if (files.nonEmpty) {
      implicit val ec = mkDaemonThreadPool(math.min(files.length, maxThreads), "hdfs-par-cp")
      val futures = files.map { f => asyncCopy(f, extractTableName(f.getName), validationPhase) }
      files.map(_.getName).zip(waitForFutures(futures, shutdown = true)).map(TpcDsTableGenResults(_))
    } else Seq.empty
  }

  private[tpcds] def mkCmd(topt: TableOptions, child: Int, outputDir: String, validate: Boolean = false) = {
    val cmd = Seq(
      s"./dsdgen",
      "-scale", s"$tpcDsScale",
      "-rngseed", s"$tpcDsRngSeed",
      "-table", topt.name,
      "-dir", s"../$outputDir"
    )
    if (validate) cmd ++ Seq("-validate")
    else {
      topt.partitions.foldLeft(cmd) { case (acc, partitions) =>
        acc ++ Seq(
          "-child", s"$child",
          "-parallel", s"$partitions"
        )
      }
    }
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
    results.foreach { i => log.info(i.toString) }
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

  private[tpcds] def fixupPath(path: String): String = {
    if (path.nonEmpty && path.endsWith("/")) path
    else if (path.nonEmpty) path + "/"
    else path
  }

  private def genData(topt: TableOptions, child: Int, validationPhase: Boolean): Seq[TpcDsTableGenResults] = {
    val kitDir = mkKitDir(tpcDsKitDir)
    val outputDir =
      if (validationPhase) s"$dsdgenValidationOutputPath${topt.name}"
      else s"$dsdgenOutputPath${topt.name}$child"
    val f = new File(s"$kitDir/$outputDir")
    f.mkdirs()
    if (runCmd(mkCmd(topt, child, outputDir, validationPhase), Some(s"$kitDir/tools"))) copyToHdfs(f, validationPhase)
    else throw new RuntimeException(s"Data generation failed for $topt, $child")
  }

  private def genData(topt: TableOptions, validationPhase: Boolean)
      (implicit spark: SparkSession, exSvc: ExSvc): Future[RDD[Seq[TpcDsTableGenResults]]] = Future {
    log.info(s"Generating ${if (validationPhase) "validation " else " "}data for $topt")
    val numPartitions = if (validationPhase) 1 else topt.partitions.getOrElse(1)
    spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map(genData(topt, _, validationPhase))
  }

  private[tpcds] def genDataWithTiming(topts: Seq[TableOptions], validationPhase: Boolean)
      (implicit spark: SparkSession, exSvc: ExSvc) = {
    topts.filterNot(_.skipgen.getOrElse(false)).map { topt =>
      val (t, res) = time(genData(topt, validationPhase))
      (topt.name, t, res)
    }
  }

  private def genData(validationPhase: Boolean)(implicit spark: SparkSession, exSvc: ExSvc): Seq[TpcDsTableGenStats] = {
    val seqOfRdds = genDataWithTiming(tableOptions, validationPhase)
    val rdds = waitForFutures(seqOfRdds.map(_._3))
    validateResults(rdds.flatMap(_.collect).flatten)
    seqOfRdds.map(TpcDsTableGenStats(_))
  }

  private def addRowCountsToStats(
      stats: Seq[TpcDsTableGenStats],
      rowCounts: Seq[(String, Long)]) = stats.map { r =>
    rowCounts.find(_._1 == r.table) match {
      case Some(rc) => r.copy(rowCount = rc._2)
      case _ => r
    }
  }

  private[tpcds] def validateTable(table: String)(implicit spark: SparkSession) = {
    createTable(table, validationPhase = true)
    val validationTable = spark.sql(s"select * from $validationDbName.${table}_text")
    validationTable.cache()
    val vtCount = validationTable.count()
    val dataTable = spark.sql(s"select * from $dbName.$table")
    val pks = tableOptions.find(_.name == table).map(_.primaryKeys).getOrElse(Set.empty).toSeq
    val res = validationTable.join(dataTable, pks)
    val joinCount = res.count()
    if (joinCount != vtCount) log.error(s"Validation failed for $table. joinCount: $joinCount, expected: $vtCount")
    validationTable.unpersist()
  }

  private def validateDatabase()(implicit spark: SparkSession, exSvc: ExSvc): Unit = {
    if (tpcDsValidate) {
      genData(validationPhase = true)
      spark.sql(s"CREATE DATABASE $validationDbName")
      spark.sql(s"USE $validationDbName")
      tables.foreach(validateTable)
      spark.sql(s"DROP DATABASE IF EXISTS $validationDbName CASCADE")
    }
  }

  private def doWorkload()(implicit spark: SparkSession, exSvc: ExSvc) = {
    val stats = genData(validationPhase = false)
    createDatabase()
    // TODO: optimization possible here.
    // create the tables (convert from CSV to parquet) right after each table has been created
    tables.foreach(createTable(_, validationPhase = false))
    val rowCounts = tables.map(validateRowCount)
    val statsWithRowCounts = addRowCountsToStats(stats, rowCounts)
    validateDatabase()
    statsWithRowCounts
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    implicit val explicitlyDeclaredImplicitSpark = spark
    implicit val ec = mkDaemonThreadPool(tableOptions.length, "tpcds-gen")
    val statsWithRowCounts =
      try doWorkload()
      finally ec.shutdown()
    spark.sparkContext.parallelize(statsWithRowCounts).toDF
  }
}
