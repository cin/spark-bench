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

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase
import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class TpcDsDataGenTest extends FlatSpec with Matchers with BeforeAndAfterEach {

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

  private val confMapTest: Map[String, Any] = Map(
    "output" -> "test-output",
    "journeydir" -> "test-journey",
    "dbname" -> "testdb",
    "warehouse" -> "spark-warehouse",
    "clean" -> false,
    "fsprefix" -> "hdfs://localhost:9000/",
    "tpcds-kit-dir" -> "kit-dir",
    "tpcds-scale" -> 3,
    "tpcds-rngseed" -> 8,
    "table-options" -> tableOptions
  )

  "TpcDsDataGenTest" should "initialize properly given sane inputs" in {
    val workload = TpcDsDataGen(confMapTest).asInstanceOf[TpcDsDataGen]
    workload.input should not be defined
    workload.output shouldBe Some("test-output")
    workload.journeyDir shouldBe "test-journey"
    workload.dbName shouldBe "testdb"
    workload.warehouse shouldBe "spark-warehouse"
    workload.clean shouldBe false
    workload.fsPrefix shouldBe "hdfs://localhost:9000/"
    workload.tpcDsKitDir shouldBe Some("kit-dir")
    workload.tpcDsScale shouldBe 3
    workload.tpcDsRngSeed shouldBe 8
    workload.tableOptions should have size 21
    // TableOptionsTest validations proper parsing of table-options
  }

  it should "syncCopy files to HDFS" in {
    val workload = TpcDsDataGen(confMapTest).asInstanceOf[TpcDsDataGen]
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
    val workload = TpcDsDataGen(confMapTest).asInstanceOf[TpcDsDataGen]
    val dstDir = new Path(workload.getOutputDataDir)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    val tmpFile = java.nio.file.Files.createTempFile("foo", "tmp")

    var createdDir = false
    if (!dstFs.isDirectory(dstDir)) {
      dstFs.mkdirs(dstDir)
      createdDir = true
    }

    implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(1))
    val f = workload.asyncCopy(tmpFile.toFile)
    val results = workload.waitForFutures(Seq(f))
    results.forall(_ == true) shouldBe true
    if (createdDir) dstFs.delete(dstDir, true)
  }

  it should "delete local directories" in {
    val dir = java.nio.file.Files.createTempDirectory("foo")
    val dirName = dir.toFile.getAbsolutePath

    val f = new java.io.File(dirName)
    f.isDirectory shouldBe true

    val workload = TpcDsDataGen(confMapTest).asInstanceOf[TpcDsDataGen]
    workload.deleteLocalDir(dirName)

    val f1 = new java.io.File(dirName)
    f1.exists shouldBe false
    f1.isDirectory shouldBe false
  }

  it should "get the right output data dir when kit dir is set" in {
    val workload = TpcDsDataGen(confMapTest).asInstanceOf[TpcDsDataGen]
    workload.getOutputDataDir shouldBe "hdfs://localhost:9000/test-output"
  }

  it should "get the right output data dir when kit dir is not set" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
    workload.getOutputDataDir shouldBe "hdfs://localhost:9000/test-journey/src/data"
  }

  it should "mkCmd without partitions" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
    val cmd = workload.mkCmd("kit-dir", TableOptions("foo", None, Seq.empty), 0, "test-output")
    val expected = Seq(
      s"kit-dir/tools/dsdgen",
      "-sc", "3",
      "-distributions", "kit-dir/tools/tpcds.idx",
      "-rngseed", "8",
      "-table", "foo",
      "-dir", "test-output"
    )
    cmd shouldBe expected
  }

  it should "mkCmd with partitions" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
    val cmd = workload.mkCmd("kit-dir", TableOptions("foo", Some(8), Seq.empty), 2, "test-output") // scalastyle:ignore
    val expected = Seq(
      s"kit-dir/tools/dsdgen",
      "-sc", "3",
      "-distributions", "kit-dir/tools/tpcds.idx",
      "-rngseed", "8",
      "-table", "foo",
      "-dir", "test-output",
      "-child", "2",
      "-parallel", "8"
    )
    cmd shouldBe expected
  }

  it should "runCmd successfully" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
    workload.runCmd(Seq("ls", "-al")) shouldBe true
  }

  it should "runCmd and fail appropriately" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
    workload.runCmd(Seq("thisisnota", "-command")) shouldBe false
  }

  it should "fixupOutputDirPath without a trailing slash" in {
    val workload = TpcDsDataGen(confMapTest).asInstanceOf[TpcDsDataGen]
    workload.fixupOutputDirPath shouldBe "test-output/"
  }

  it should "fixupOutputDirPath with a trailing slash" in {
    val workload = TpcDsDataGen(confMapTest + ("output" -> "test-output/")).asInstanceOf[TpcDsDataGen]
    workload.fixupOutputDirPath shouldBe "test-output/"
  }

  it should "fixupOutputDirPath with an empty output path" in {
    val workload = TpcDsDataGen(confMapTest + ("output" -> "")).asInstanceOf[TpcDsDataGen]
    workload.fixupOutputDirPath shouldBe ""
  }

  it should "fixupOutputDirPath with no output" in {
    val workload = TpcDsDataGen(confMapTest - "output").asInstanceOf[TpcDsDataGen]
    workload.fixupOutputDirPath shouldBe ""
  }

  it should "validateResults successfully" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
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
    workload.validateResults(results)
  }

  it should "fail to validateResults when appropriate" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
    val thrown = the[RuntimeException] thrownBy workload.validateResults(Seq.empty)
    thrown.getMessage shouldBe "Data generation failed for tpcds. Check the executor logs for details."
  }

  it should "fail to validateResults when any table fails" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
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
    val thrown = the[RuntimeException] thrownBy workload.validateResults(results)
    thrown.getMessage shouldBe "Not all tables are present in the output. Check the executor logs for more details."
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  // integration type tests
  /////////////////////////////////////////////////////////////////////////////////////////////

  private def checkCleanup(): Unit = {
    val journeyDir = confMapTest("journeydir").asInstanceOf[String]
    val fsPrefix = confMapTest("fsprefix").asInstanceOf[String]

    val f = new java.io.File(journeyDir)
    f.exists shouldBe false
    f.isDirectory shouldBe false

    val dstDir = new Path(s"$fsPrefix$journeyDir")
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    dstFs.exists(dstDir) shouldBe false
    dstFs.isDirectory(dstDir) shouldBe false
  }

  // ignore the cleanup tests so the journey won't have to be downloaded every time the tests run
  ignore should "cleanup the journey from the local filesystem and HDFS initially" in {
    val workload = TpcDsDataGen(confMapTest + ("output" -> "")).asInstanceOf[TpcDsDataGen]
    workload.cleanup()
    checkCleanup()
  }

  it should "retrieve the journey's repository" in {
    val workload = TpcDsDataGen(confMapTest).asInstanceOf[TpcDsDataGen]
    val journeyDir = confMapTest("journeydir").asInstanceOf[String]
    val fsPrefix = confMapTest("fsprefix").asInstanceOf[String]

    workload.retrieveRepo()

    val f = new java.io.File(journeyDir)
    f.exists shouldBe true
    f.isDirectory shouldBe true

    val dstDir = new Path(s"$fsPrefix$journeyDir")
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    dstFs.isDirectory(dstDir) shouldBe true
  }

  it should "createDatabase based on the journey" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
    implicit val spark = SparkSessionProvider.spark
    workload.createDatabase
    val dbs = spark.sql("show databases").collect
    dbs.find(_.getAs[String]("databaseName") == "testdb") shouldBe defined
  }

  it should "createTables based on the journey" in {
    val workload = TpcDsDataGen(confMapTest - "tpcds-kit-dir").asInstanceOf[TpcDsDataGen]
    implicit val spark = SparkSessionProvider.spark
    val tableName = TpcDsBase.tables.head
    workload.createTable(tableName)
    val tables = spark.sql("show tables").collect
    tables.find(_.getAs[String]("tableName") == tableName) shouldBe defined
  }

  ignore should "cleanup the journey from the local filesystem and HDFS" in {
    val workload = TpcDsDataGen(confMapTest + ("output" -> "")).asInstanceOf[TpcDsDataGen]
    workload.cleanup()
    checkCleanup()
  }
}
