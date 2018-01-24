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

import java.util.concurrent.Executors.newFixedThreadPool

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.tables
import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class TpcDsDataGenTest extends FlatSpec with Matchers {
  private val tableOptions = """[
    {
      "name": "call_center"
    },
    {
      "name": "catalog_page"
    },
    {
      "name": "catalog_sales"
      "partitions": 10
      "partitionColumns": ["cs_sold_date_sk"]
    },
    {
      "name": "customer"
    },
    {
      "name": "customer_address"
    },
    {
      "name": "customer_demographics"
    },
    {
      "name": "date_dim"
    },
    {
      "name": "income_band"
    },
    {
      "name": "inventory"
      "partitions": 10
      "partitionColumns": ["inv_date_sk"]
    },
    {
      "name": "item"
    },
    {
      "name": "household_demographics"
    },
    {
      "name": "promotion"
    },
    {
      "name": "reason"
    },
    {
      "name": "ship_mode"
    },
    {
      "name": "store"
    },
    {
      "name": "store_sales"
      "partitions": 10
      "partitionColumns": ["ss_sold_date_sk"]
    },
    {
      "name": "time_dim"
    },
    {
      "name": "warehouse"
    },
    {
      "name": "web_page"
    },
    {
      "name": "web_sales"
      "partitions": 10
      "partitionColumns": ["ws_sold_date_sk"]
    },
    {
      "name": "web_site"
    }
  ]"""

  private implicit val conf = new Configuration

  private val dbName = "testdb"

  // TODO: is there a less brittle way to do this?
  private val cwd = new java.io.File(".").getCanonicalPath
  private val kitDir = s"$cwd/cli/src/test/resources/tpcds/${
    sys.props("os.name") match {
      case "Linux" => "linux"
      case _ => "osx"
    }
  }"

  private val confMapTest: Map[String, Any] = Map(
    "output" -> "test-output",
    "dbname" -> dbName,
    "warehouse" -> "spark-warehouse",
    "clean" -> false,
    "fsprefix" -> "hdfs://localhost:9000/",
    "tpcds-kit-dir" -> kitDir,
    "tpcds-scale" -> 1,
    "tpcds-rngseed" -> 8,
    "table-options" -> tableOptions
  )

  private def mkWorkload: TpcDsDataGen = mkWorkload(confMapTest)
  private def mkWorkload(confMap: Map[String, Any]): TpcDsDataGen =
    TpcDsDataGen(confMap).asInstanceOf[TpcDsDataGen]

  "TpcDsDataGenTest" should "initialize properly given sane inputs" in {
    val workload = mkWorkload
    workload.input should not be defined
    workload.output shouldBe Some("test-output")
    workload.dbName shouldBe "testdb"
    workload.warehouse shouldBe "spark-warehouse"
    workload.clean shouldBe false
    workload.fsPrefix shouldBe "hdfs://localhost:9000/"
    workload.tpcDsKitDir shouldBe kitDir
    workload.tpcDsScale shouldBe 1
    workload.tpcDsRngSeed shouldBe 8
    workload.tableOptions should have size 21
    // TableOptionsTest validations proper parsing of table-options
  }

  it should "syncCopy files to HDFS" in {
    val workload = mkWorkload
    val tmpFile = java.nio.file.Files.createTempFile("foo", "tmp")
    val dirString = "hdfs://localhost:9000/foo-tmp"
    val dstDir = new Path(dirString)

    val dstFs = FileSystem.get(dstDir.toUri, conf)
    var createdDir = false
    if (!dstFs.isDirectory(dstDir)) {
      dstFs.mkdirs(dstDir)
      createdDir = true
    }

    workload.syncCopy(tmpFile.toFile, dirString) shouldBe true

    if (createdDir) dstFs.delete(dstDir, true)
  }

  it should "asyncCopy" in {
    implicit val conf = new Configuration
    val workload = mkWorkload
    val outputDir = workload.getOutputDataDir
    val dstDir = new Path(outputDir)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    val tmpFile = java.nio.file.Files.createTempFile("foo", "tmp")

    var createdDir = false
    if (!dstFs.isDirectory(dstDir)) {
      dstFs.mkdirs(dstDir)
      createdDir = true
    }

    implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(1))
    val f = workload.asyncCopy(tmpFile.toFile, "foo")
    val results = workload.waitForFutures(Seq(f))
    results.forall(_ == true) shouldBe true

    val dstFn = new Path(s"$outputDir/foo", tmpFile.toFile.getName)
    val dstFs1 = FileSystem.get(dstDir.toUri, conf)
    dstFs1.exists(dstFn) shouldBe true
    if (createdDir) dstFs.delete(dstDir, true)
  }

  it should "delete local directories" in {
    val dir = java.nio.file.Files.createTempDirectory("foo")
    val dirName = dir.toFile.getAbsolutePath

    val f = new java.io.File(dirName)
    f.isDirectory shouldBe true

    val workload = mkWorkload
    workload.deleteLocalDir(dirName)

    val f1 = new java.io.File(dirName)
    f1.exists shouldBe false
    f1.isDirectory shouldBe false
  }

  it should "get the right output data dir when kit dir is set" in {
    val workload = mkWorkload
    workload.getOutputDataDir shouldBe "hdfs://localhost:9000/test-output"
  }

  it should "mkCmd without partitions" in {
    val workload = mkWorkload
    val cmd = workload.mkCmd(TableOptions("foo", None, Seq.empty), 0, "test-output")
    val expected = Seq(
      s"$kitDir/tools/dsdgen",
      "-sc", "1",
      "-distributions", s"$kitDir/tools/tpcds.idx",
      "-rngseed", "8",
      "-table", "foo",
      "-dir", "test-output"
    )
    cmd shouldBe expected
  }

  it should "mkCmd with partitions" in {
    val workload = mkWorkload
    val cmd = workload.mkCmd(TableOptions("foo", Some(8), Seq.empty), 2, "test-output") // scalastyle:ignore
    val expected = Seq(
      s"$kitDir/tools/dsdgen",
      "-sc", "1",
      "-distributions", s"$kitDir/tools/tpcds.idx",
      "-rngseed", "8",
      "-table", "foo",
      "-dir", "test-output",
      "-child", "2",
      "-parallel", "8"
    )
    cmd shouldBe expected
  }

  it should "fixupOutputDirPath without a trailing slash" in {
    val workload = mkWorkload
    workload.fixupOutputDirPath shouldBe "test-output/"
  }

  it should "fixupOutputDirPath with a trailing slash" in {
    val workload = mkWorkload(confMapTest + ("output" -> "test-output/"))
    workload.fixupOutputDirPath shouldBe "test-output/"
  }

  it should "fixupOutputDirPath with an empty output path" in {
    val workload = mkWorkload(confMapTest + ("output" -> ""))
    workload.fixupOutputDirPath shouldBe ""
  }

  it should "fixupOutputDirPath with no output" in {
    val workload = mkWorkload(confMapTest - "output")
    workload.fixupOutputDirPath shouldBe ""
  }

  it should "validateResults successfully" in {
    val workload = mkWorkload
    val results = Seq(
      TpcDsTableGenResults("call_center", res = true),
      TpcDsTableGenResults("catalog_sales", res = true),
      TpcDsTableGenResults("customer_demographics", res = true),
      TpcDsTableGenResults("income_band", res = true),
      TpcDsTableGenResults("promotion", res = true),
      TpcDsTableGenResults("store", res = true),
      TpcDsTableGenResults("time_dim", res = true),
      TpcDsTableGenResults("web_returns", res = true),
      TpcDsTableGenResults("catalog_page", res = true),
      TpcDsTableGenResults("customer", res = true),
      TpcDsTableGenResults("date_dim", res = true),
      TpcDsTableGenResults("inventory", res = true),
      TpcDsTableGenResults("reason", res = true),
      TpcDsTableGenResults("store_returns", res = true),
      TpcDsTableGenResults("warehouse", res = true),
      TpcDsTableGenResults("web_sales", res = true),
      TpcDsTableGenResults("catalog_returns", res = true),
      TpcDsTableGenResults("customer_address", res = true),
      TpcDsTableGenResults("household_demographics", res = true),
      TpcDsTableGenResults("item", res = true),
      TpcDsTableGenResults("ship_mode", res = true),
      TpcDsTableGenResults("store_sales", res = true),
      TpcDsTableGenResults("web_page", res = true),
      TpcDsTableGenResults("web_site", res = true)
    )

    implicit val spark = SparkSessionProvider.spark
    workload.validateResults(results)
  }

  it should "fail to validateResults when appropriate" in {
    implicit val spark = SparkSessionProvider.spark
    val workload = mkWorkload
    val thrown = the[RuntimeException] thrownBy workload.validateResults(Seq.empty)
    thrown.getMessage shouldBe "Data generation failed for tpcds. Check the executor logs for details."
  }

  it should "fail to validateResults when any table fails" in {
    val workload = mkWorkload
    val results = Seq(
      TpcDsTableGenResults("call_center", res = true),
      TpcDsTableGenResults("catalog_sales", res = true),
      TpcDsTableGenResults("customer_demographics", res = true),
      TpcDsTableGenResults("income_band", res = true),
      TpcDsTableGenResults("promotion", res = false),
      TpcDsTableGenResults("store", res = true),
      TpcDsTableGenResults("time_dim", res = true),
      TpcDsTableGenResults("web_returns", res = true),
      TpcDsTableGenResults("catalog_page", res = true),
      TpcDsTableGenResults("customer", res = true),
      TpcDsTableGenResults("date_dim", res = true),
      TpcDsTableGenResults("inventory", res = true),
      TpcDsTableGenResults("reason", res = true),
      TpcDsTableGenResults("store_returns", res = true),
      TpcDsTableGenResults("warehouse", res = true),
      TpcDsTableGenResults("web_sales", res = true),
      TpcDsTableGenResults("catalog_returns", res = true),
      TpcDsTableGenResults("customer_address", res = true),
      TpcDsTableGenResults("household_demographics", res = true),
      TpcDsTableGenResults("item", res = true),
      TpcDsTableGenResults("ship_mode", res = true),
      TpcDsTableGenResults("store_sales", res = true),
      TpcDsTableGenResults("web_page", res = true),
      TpcDsTableGenResults("web_site", res = true)
    )

    implicit val spark = SparkSessionProvider.spark
    val thrown = the[RuntimeException] thrownBy workload.validateResults(results)
    thrown.getMessage shouldBe "Not all tables are present in the output. Check the executor logs for more details."
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  // integration type tests
  /////////////////////////////////////////////////////////////////////////////////////////////

//  private def checkCleanup(): Unit = {
//    val journeyDir = confMapTest("journeydir").asInstanceOf[String]
//    val fsPrefix = confMapTest("fsprefix").asInstanceOf[String]
//
//    val f = new java.io.File(journeyDir)
//    f.exists shouldBe false
//    f.isDirectory shouldBe false
//
//    val dstDir = new Path(s"$fsPrefix$journeyDir")
//    val dstFs = FileSystem.get(dstDir.toUri, conf)
//    dstFs.exists(dstDir) shouldBe false
//    dstFs.isDirectory(dstDir) shouldBe false
//  }

  // ignore the cleanup tests so the journey won't have to be downloaded every time the tests run
//  it should "cleanup the journey from the local filesystem and HDFS initially" in {
//    implicit val conf = new Configuration()
//    val workload = mkWorkload(confMapTest + ("clean" -> true))
//    workload.cleanupJourney
//    checkCleanup()
//  }
//
//  it should "determine if the journey exists after cleanup" in {
//    implicit val conf = new Configuration()
//    val workload = mkWorkload(confMapTest + ("clean" -> true))
//    workload.doesJourneyExist shouldBe false
//  }

//  it should "retrieve the journey's repository" in {
//    val workload = mkWorkload
//    val journeyDir = confMapTest("journeydir").asInstanceOf[String]
//    val fsPrefix = confMapTest("fsprefix").asInstanceOf[String]
//
//    workload.retrieveRepo()
//
//    val f = new java.io.File(journeyDir)
//    f.exists shouldBe true
//    f.isDirectory shouldBe true
//
//    val dstDir = new Path(s"$fsPrefix$journeyDir")
//    val dstFs = FileSystem.get(dstDir.toUri, conf)
//    dstFs.isDirectory(dstDir) shouldBe true
//  }
//
//  it should "determine if the journey exists after cloning" in {
//    implicit val conf = new Configuration()
//    val workload = mkWorkload
//    workload.doesJourneyExist shouldBe true
//  }
//
//  it should "createDatabase based on the journey" in {
//    implicit val spark = SparkSessionProvider.spark
//    val workload = mkWorkload
//    workload.createDatabase
//    val dbs = spark.sql("show databases").collect
//    dbs.find(_.getAs[String]("databaseName") == "testdb") shouldBe defined
//  }
//
//  it should "createTables based on the journey" in {
//    implicit val spark = SparkSessionProvider.spark
//    val workload = mkWorkload
//    val tableName = com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.tables.head
//    workload.createTable(tableName)
//    val tables = spark.sql("show tables").collect
//    tables.find(_.getAs[String]("tableName") == tableName) shouldBe defined
//  }

  private def cleanupOutput(workload: TpcDsDataGen): Unit = {
    val hdfsDataDir = workload.getOutputDataDir
    val dstDir = new Path(hdfsDataDir)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    dstFs.delete(dstDir, true)
    workload.deleteLocalDir(workload.fixupOutputDirPath)
  }

  private def genDataTest(tableName: String, numPartitions: Option[Int]): Unit = {
    val workload = mkWorkload

    cleanupOutput(workload)

    implicit val spark = SparkSessionProvider.spark
    implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(numPartitions.getOrElse(1)))
    val (dur, results) = time(workload.genDataWithTiming(Seq(TableOptions(tableName, numPartitions, Seq.empty))))
    results should have size 1
    results.head._1 shouldBe tableName
    results.head._2 should be > 0L
    results.head._2 should be <= dur
    val r0 = workload.waitForFutures(Seq(results.head._3))
    r0 should have size 1
    r0.foreach { r =>
      val r1 = r.collect.flatten
      numPartitions match {
        case Some(np) =>
          r1 should have size np
          r1.zipWithIndex.foreach { case (rr, i) =>
            rr.table shouldBe s"${tableName}_${i + 1}_$np.dat"
            rr.res shouldBe true
          }
        case _ =>
          r1 should have size 1
          r1.head.table shouldBe s"$tableName.dat"
          r1.head.res shouldBe true
      }
    }

    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    spark.sql(s"CREATE DATABASE $dbName")
    spark.sql(s"USE $dbName")

    workload.createTable(tableName)
    workload.validateRowCount(tableName)

    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    workload.deleteTableFromDisk(tableName)
  }

  it should "genData using the TPC-DS kit with no partitioning" in {
    genDataTest("call_center", None)
  }

  it should "genData using the TPC-DS kit with partitioning" in {
    genDataTest("inventory", Some(10)) // scalastyle:ignore
  }

  it should "genData using the TPC-DS kit for promotion" in {
    genDataTest("promotion", None)
  }

  it should "cleanup output" in {
    cleanupOutput(mkWorkload)
  }

  it should "doWorkload" in {
    val workload = mkWorkload(confMapTest + ("clean" -> true))
    val spark = SparkSessionProvider.spark
    workload.doWorkload(None, spark).show(tables.length)
  }

  it should "cleanup output when done" in {
    cleanupOutput(mkWorkload)
  }
}
