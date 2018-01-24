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

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.{getOrDefault, getOrThrowT, optionallyGet, runCmd, time}
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}

case class TpcDsQueryGenStats(res: Boolean, duration: Long)

object TpcDsQueryGen extends WorkloadDefaults {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val name = "tpcdsquerygen"

  private val rngSeedDefault = 100
  private val scaleDefault = 1
  private val streamsDefault = 1

  def apply(m: Map[String, Any]): Workload = TpcDsQueryGen(
    optionallyGet(m, "input"),
    optionallyGet(m, "output"),
    getOrThrowT[String](m, "tpcds-kit-dir"),
    getOrDefault[Int](m, "tpcds-scale", scaleDefault),
    getOrDefault[Int](m, "tpcds-streams", streamsDefault),
    optionallyGet[Int](m, "tpcds-count"),
    getOrDefault[Int](m, "tpcds-rngseed", rngSeedDefault),
    getOrDefault[String](m, "tpcds-dialect", "ansi")
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

  private[tpcds] def mkCmd: Seq[String] = {
    val cmd = Seq(
      s"$tpcDsKitDir/tools/dsqgen",
      "-sc", s"$tpcDsScale",
      "-distributions", s"$tpcDsKitDir/tools/tpcds.idx",
      "-dialect", tpcDsDialect,
      "-rngseed", s"$tpcDsRngSeed",
      "-dir", s"$tpcDsKitDir/query_templates/",
      "-input", s"$tpcDsKitDir/query_templates/templates.lst",
      "-output_dir", output.get,
      "-streams", s"$tpcDsStreams"
    )
    tpcDsCount.foldLeft(cmd) { case (acc, cnt) => acc ++ Seq("-count", s"$cnt") }
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    val f = new File(output.get)
    if (!f.exists) f.mkdirs
    log.debug(s"Outputting data to ${f.getAbsolutePath}")
    val (dur, res) = time(runCmd(mkCmd))
    spark.sparkContext.parallelize(Seq(TpcDsQueryGenStats(res, dur))).toDF
  }
}
