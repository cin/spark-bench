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

import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import org.scalatest.{FlatSpec, Matchers}

class TpcDsQueryGenTest extends FlatSpec with Matchers {
  private val cwd = sys.props("user.dir")
  private val kitDir = s"$cwd/cli/src/test/resources/tpcds/${
    sys.props("os.name") match {
      case "Linux" => "linux"
      case _ => "osx"
    }
  }"

  private val outputPath = java.nio.file.Files.createTempDirectory("qgen")
  private val outputDir = outputPath.toFile.getAbsolutePath

  private val confMapTest: Map[String, Any] = Map(
    "output" -> outputDir,
    "tpcds-kit-dir" -> kitDir,
    "tpcds-scale" -> 1,
    "tpcds-streams" -> 1,
    "tpcds-rngseed" -> 8
  )

  private def mkWorkload: TpcDsQueryGen = mkWorkload(confMapTest)
  private def mkWorkload(confMap: Map[String, Any]): TpcDsQueryGen =
    TpcDsQueryGen(confMap).asInstanceOf[TpcDsQueryGen]

  "TpcDsQueryGen" should "initialize properly" in {
    val workload = mkWorkload
    workload.input should not be defined
    workload.output shouldBe Some(outputDir)
    workload.tpcDsKitDir shouldBe kitDir
    workload.tpcDsKitDir shouldBe kitDir
    workload.tpcDsScale shouldBe 1
    workload.tpcDsRngSeed shouldBe 8
    workload.tpcDsStreams shouldBe 1
    workload.tpcDsCount shouldBe empty
    workload.tpcDsDialect shouldBe "spark"
  }

  it should "initialize properly given counts" in {
    val workload = mkWorkload(confMapTest + ("tpcds-count" -> 5))
    workload.input should not be defined
    workload.output shouldBe Some(outputDir)
    workload.tpcDsKitDir shouldBe kitDir
    workload.tpcDsKitDir shouldBe kitDir
    workload.tpcDsScale shouldBe 1
    workload.tpcDsRngSeed shouldBe 8
    workload.tpcDsStreams shouldBe 1
    workload.tpcDsCount shouldBe Some(5) //scalastyle:ignore
    workload.tpcDsDialect shouldBe "spark"
  }

  it should "mkCmd" in {
    val workload = mkWorkload
    val cmd = workload.mkCmd
    val expected = Seq(
      s"./dsqgen",
      "-sc", "1",
      "-distributions", s"tpcds.idx",
      "-dialect", "spark",
      "-rngseed", "8",
      "-dir", s"../query_templates",
      "-input", s"../query_templates/templates.lst",
      "-output_dir", outputDir,
      "-streams", "1"
    )
    cmd shouldBe expected
  }

  it should "mkCmd with count" in {
    val workload = mkWorkload(confMapTest + ("tpcds-count" -> 5))
    val cmd = workload.mkCmd
    val expected = Seq(
      s"./dsqgen",
      "-sc", "1",
      "-distributions", s"tpcds.idx",
      "-dialect", "spark",
      "-rngseed", "8",
      "-dir", s"../query_templates",
      "-input", s"../query_templates/templates.lst",
      "-output_dir", outputDir,
      "-streams", "1",
      "-count", "5"
    )
    cmd shouldBe expected
  }

  // integration tests
  it should "doWorkload" in {
    val workload = mkWorkload
    val spark = SparkSessionProvider.spark
    val df = workload.doWorkload(None, spark)
    df.collect().head.getAs[Boolean](0) shouldBe true
    df.show()
    val f = new java.io.File(outputDir)
    f.listFiles.flatMap {
      case ff if ff.getName == "query_0.sql" => Some(ff.getName)
      case _ => None
    } should have size 1
  }

  it should "doWorkload with multiple streams" in {
    val workload = mkWorkload(confMapTest + ("tpcds-streams" -> 4))
    val spark = SparkSessionProvider.spark
    val df = workload.doWorkload(None, spark)
    df.collect().head.getAs[Boolean](0) shouldBe true
    df.show()
    val f = new java.io.File(outputDir)
    f.listFiles.flatMap {
      case ff if ff.getName.startsWith("query_") => Some(ff.getName)
      case _ => None
    } should have size 4
  }
}
