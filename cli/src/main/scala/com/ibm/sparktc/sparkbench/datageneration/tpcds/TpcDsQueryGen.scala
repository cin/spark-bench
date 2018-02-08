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

package com.ibm.sparktc.sparkbench.datageneration.tpcds

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.nio.file.Files
import java.nio.file.StandardCopyOption.REPLACE_EXISTING

import scala.io.Source.fromFile

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.{getOrDefault, getOrThrowT, optionallyGet, runCmd, time}
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.syncCopy

case class TpcDsQueryGenStats(res: Boolean, duration: Long)

object TpcDsQueryGen extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val name = "tpcdsquerygen"

  private val rngSeedDefault = 100
  private val scaleDefault = 1
  private val streamsDefault = 1

  private val DazeAddRgx = """^(.*)(cast\s*\('[0-9-]{8,10}'\s+as\s+date\))\s*\+\s*([0-9]+)\s+days(.*)$""".r
  private val DazeSubRgx = """^(.*)(cast\s*\('[0-9-]{8,10}'\s+as\s+date\))\s*\-\s*([0-9]+)\s+days(.*)$""".r
  private val NameWithSpacesRgx = """^(.*)"([a-zA-Z0-9\- >]+)"(.*)$""".r
  private val WeirdConcatRgx = """^(.*)'([a-zA-Z0-9_-]+)'\s*\|\|\s*([a-zA-Z0-9_-]+)\s+as\s+([a-zA-Z0-9_-]+)(.*)$""".r
  private val MoarConcatRgx = """^(\s+),\s*(['-z]+)\s*\|\|(.*)\s*\|\|\s*(['-z]+)\s+as\s+(['-z]+)$""".r
  private val LastReviewRgx = """^(.*)c_last_review_date_sk(.*)$""".r
  private val CatalogSalesStartRgx = """^(\s+from\s+)(select ws_sold_date_sk sold_date_sk)$""".r
  private val CatalogSalesEndRgx = """^(\s+from catalog_sales\)),$""".r

  def apply(m: Map[String, Any]): Workload = TpcDsQueryGen(
    optionallyGet(m, "input"),
    optionallyGet(m, "output"),
    getOrThrowT[String](m, "tpcds-kit-dir"),
    getOrDefault[Int](m, "tpcds-scale", scaleDefault),
    getOrDefault[Int](m, "tpcds-streams", streamsDefault),
    optionallyGet[Int](m, "tpcds-count"),
    getOrDefault[Int](m, "tpcds-rngseed", rngSeedDefault),
    getOrDefault[String](m, "tpcds-dialect", "spark")
  )
}

case class TpcDsQueryGen(
  input: Option[String],
  output: Option[String],
  tpcDsKitDir: String,
  tpcDsScale: Int,
  tpcDsStreams: Int,
  tpcDsCount: Option[Int],
  tpcDsRngSeed: Int,
  tpcDsDialect: String
) extends Workload {
  import TpcDsQueryGen._

  private[tpcds] def mkCmd(outputDir: String): Seq[String] = {
    val cmd = Seq(
      s"./dsqgen",
      "-sc", s"$tpcDsScale",
      "-distributions", s"tpcds.idx",
      "-dialect", tpcDsDialect,
      "-rngseed", s"$tpcDsRngSeed",
      "-dir", s"../query_templates",
      "-input", s"../query_templates/templates.lst",
      "-output_dir", outputDir,
      "-streams", s"$tpcDsStreams"
    )
    tpcDsCount.foldLeft(cmd) { case (acc, cnt) => acc ++ Seq("-count", s"$cnt") }
  }

  private def fixQueries(f: File): Iterator[String] = fromFile(f).getLines.map {
    case DazeAddRgx(before, dt, days, after) => s"${before}date_add($dt, $days)$after"
    case DazeSubRgx(before, dt, days, after) => s"${before}date_sub($dt, $days)$after"
    case NameWithSpacesRgx(before, nws, after) => s"$before`$nws`$after"
    case WeirdConcatRgx(before, arg1, arg2, ccname, after) => s"${before}concat('$arg1', $arg2) as $ccname$after"
    case MoarConcatRgx(one, two, three, four, five) => s"$one, concat($two, $three, $four) as $five"
    case LastReviewRgx(before, after) => s"${before}c_last_review_date$after"
    case CatalogSalesStartRgx(before, after) => s"$before($after"
    case CatalogSalesEndRgx(before) => s"$before),"
    case line => line
  }

  private def mkSparkSqlCompliant(f: File): Unit = f.listFiles.foreach {
    case ff if ff.getName.endsWith(".sql") =>
      val lines = fixQueries(ff)
      val newf = new File(s"${ff.getParent}/${ff.getName}.tmp")
      val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newf)))
      lines.foreach { l => bw.write(l); bw.newLine() }
      bw.flush()
      bw.close()
      Files.move(newf.toPath, ff.toPath, REPLACE_EXISTING)
      if (output.get.startsWith("hdfs://")) syncCopy(ff, output.get)
    case _ => () // do nothing if there are other non-sql files in this directory
  }

  /**
    * If the output directory is an HDFS location, create a temporary directory to which
    * dsqgen can write its output. Otherwise, ensure the output directory exists and create
    * it if not.
    */
  private def mkOutputDir(): File = {
    if (output.get.startsWith("hdfs://")) {
      Files.createTempDirectory("dsqgen").toFile
    } else {
      val f = new File(output.get)
      if (!f.exists) f.mkdirs
      f
    }
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    val f = mkOutputDir()
    log.debug(s"Outputting data to ${f.getAbsolutePath}")
    val (dur, res) = time(runCmd(mkCmd(f.getAbsolutePath), Some(s"$tpcDsKitDir/tools")))
    mkSparkSqlCompliant(f)
    spark.sparkContext.parallelize(Seq(TpcDsQueryGenStats(res, dur))).toDF
  }
}
