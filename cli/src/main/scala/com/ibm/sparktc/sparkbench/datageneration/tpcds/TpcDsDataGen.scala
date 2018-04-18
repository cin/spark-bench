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
      getOrDefault[String](m, "tpcds-data-validation-output", s"$dataOutput-validation"),
      getOrDefault[Boolean](m, "tpcds-create-tables-only", false),
      optionallyGet(m, "tpcds-spark-cassandra-host"),
      optionallyGet(m, "tpcds-spark-cassandra-username"),
      optionallyGet(m, "tpcds-sparkk-cassandra-password")
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
    tpcDsDataValidationOutput: String,
    tpcDsCreateTablesOnly: Boolean,
    tpcDsSparkCassandraHost: Option[String],
    tpcDsSparkCassandraUserName: Option[String],
    tpcDsSparkCassandraPassword: Option[String]
  ) extends Workload {
  import TpcDsDataGen._

  private[tpcds] val dsdgenOutputPath: String = fixupPath(tpcDsDsdgenOutput)
  val dsdgenValidationOutputPath: String = fixupPath(tpcDsDsdgenValidationOutput)
  private[tpcds] val validationDbName = s"${dbName}_validation"

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

  private[tpcds] def asyncCopy(file: File, tableName: String, validationPhase: Boolean)(implicit ec: ExSvc) = Future {
    syncCopy(file, if (validationPhase) s"$tpcDsDataValidationOutput/$tableName" else s"$tpcDsDataOutput/$tableName")
  }.recover { case e: Throwable => log.error(s"FileUtil.copy failed on ${file.getName}", e); false }

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

  def genData(validationPhase: Boolean)(implicit spark: SparkSession, exSvc: ExSvc): Seq[TpcDsTableGenStats] = {
    val seqOfRdds = genDataWithTiming(tableOptions, validationPhase)
    val rdds = waitForFutures(seqOfRdds.map(_._3))
    validateResults(rdds.flatMap(_.collect).flatten)
    seqOfRdds.map(TpcDsTableGenStats(_))
  }

  private def doWorkload()(implicit spark: SparkSession, exSvc: ExSvc) = {
    val stats =
      if (!tpcDsCreateTablesOnly) genData(validationPhase = false)
      else Seq.empty

    implicit val thisGuy = this
    val backend = new backends.ParquetBackend()
    backend.run(stats)
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
