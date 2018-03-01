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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.{createTempDir, deleteLocalDir, syncCopy}
import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class TpcDsDataGenTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private implicit val conf = new Configuration

  private val dbName = "testdb"
  private val cwd = sys.props("user.dir")
  private val kitDir = s"$cwd/cli/src/test/resources/tpcds/${
    sys.props("os.name") match {
      case "Linux" => "linux"
      case _ => "osx"
    }
  }"

  private val confMapTest: Map[String, Any] = Map(
    "tpcds-data-output" -> "hdfs://localhost:9000/test-output",
    "tpcds-dsdgen-output" -> "test-output",
    "dbname" -> dbName,
    "warehouse" -> "spark-warehouse",
    "clean" -> false,
    "tpcds-kit-dir" -> kitDir,
    "tpcds-scale" -> 1,
    "tpcds-rngseed" -> 8,
    "table-options" -> "/tpcds/table-options.json"
  )

  override protected def beforeAll(): Unit = {
    val f = new java.io.File(s"$cwd/spark-warehouse/$dbName.db")
    if (f.exists()) deleteLocalDir(f.getCanonicalPath)
  }

  override protected def afterAll(): Unit = {
    val f = new java.io.File(s"$kitDir/test-output")
    if (f.exists()) deleteLocalDir(f.getCanonicalPath)
  }

  private def mkWorkload: TpcDsDataGen = mkWorkload(confMapTest)
  private def mkWorkload(confMap: Map[String, Any]): TpcDsDataGen =
    TpcDsDataGen(confMap).asInstanceOf[TpcDsDataGen]

  "TpcDsDataGenTest" should "initialize properly given sane inputs" in {
    val workload = mkWorkload
    workload.input should not be defined
    workload.dbName shouldBe "testdb"
    workload.warehouse shouldBe "spark-warehouse"
    workload.tpcDsKitDir shouldBe kitDir
    workload.tpcDsScale shouldBe 1
    workload.tpcDsRngSeed shouldBe 8
    workload.tpcDsDataOutput shouldBe "hdfs://localhost:9000/test-output"
    workload.tableOptions should have size 24
    // TableOptionsTest validations proper parsing of table-options
  }

  it should "syncCopy files to HDFS" in {
    val tmpFile = java.nio.file.Files.createTempFile("foo", "tmp")
    val dirString = "hdfs://localhost:9000/foo-tmp"
    val dstDir = new Path(dirString)

    val dstFs = FileSystem.get(dstDir.toUri, conf)
    var createdDir = false
    if (!dstFs.isDirectory(dstDir)) {
      dstFs.mkdirs(dstDir)
      createdDir = true
    }

    syncCopy(tmpFile.toFile, dirString) shouldBe true

    if (createdDir) dstFs.delete(dstDir, true)
  }

  it should "asyncCopy" in {
    implicit val conf = new Configuration
    val workload = mkWorkload
    val dstDir = new Path(workload.dsdgenOutputPath)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    val tmpFile = java.nio.file.Files.createTempFile("foo", "tmp")

    var createdDir = false
    if (!dstFs.isDirectory(dstDir)) {
      dstFs.mkdirs(dstDir)
      createdDir = true
    }

    implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(1))
    val f = workload.asyncCopy(tmpFile.toFile, "foo")
    val results = waitForFutures(Seq(f))
    results.foreach(_ shouldBe true)

    val dstFn = new Path(s"${workload.tpcDsDataOutput}/foo", tmpFile.toFile.getName)
    val dstFs1 = dstFn.getFileSystem(conf)
    dstFs1.exists(dstFn) shouldBe true
    if (createdDir) {
      dstFs.delete(dstDir, true)
      dstFs1.delete(dstFn, true)
    }
  }

  it should "delete local directories" in {
    val dir = createTempDir(namePrefix = "foo")
    val dirName = dir.getCanonicalPath

    val f = new java.io.File(dirName)
    f.isDirectory shouldBe true

    deleteLocalDir(dirName)

    val f1 = new java.io.File(dirName)
    f1.exists shouldBe false
    f1.isDirectory shouldBe false
  }

  it should "get the right output data dir when kit dir is set" in {
    mkWorkload.dsdgenOutputPath shouldBe "test-output/"
  }

  it should "mkCmd without partitions" in {
    val workload = mkWorkload
    val cmd = workload.mkCmd(TableOptions("foo", None, None, Seq.empty), 0, "test-output")
    val expected = Seq(
      s"./dsdgen",
      "-scale", "1",
      "-rngseed", "8",
      "-table", "foo",
      "-dir", "../test-output"
    )
    cmd shouldBe expected
  }

  it should "mkCmd with partitions" in {
    val workload = mkWorkload
    val cmd = workload.mkCmd(TableOptions("foo", None, Some(8), Seq.empty), 2, "test-output") // scalastyle:ignore
    val expected = Seq(
      s"./dsdgen",
      "-scale", "1",
      "-rngseed", "8",
      "-table", "foo",
      "-dir", "../test-output",
      "-child", "2",
      "-parallel", "8"
    )
    cmd shouldBe expected
  }

  it should "mkPartitionStmt" in {
    val workload = mkWorkload
    workload.mkPartitionStatement("catalog_sales") shouldBe "PARTITIONED BY(cs_sold_date_sk)"
    workload.mkPartitionStatement("call_center") shouldBe ""
  }

  it should "dsdgenOutputPath without a trailing slash" in {
    val workload = mkWorkload
    workload.dsdgenOutputPath shouldBe "test-output/"
  }

  it should "dsdgenOutputPath with a trailing slash" in {
    val workload = mkWorkload(confMapTest + ("tpcds-dsdgen-output" -> "test-output/"))
    workload.dsdgenOutputPath shouldBe "test-output/"
  }

  it should "dsdgenOutputPath with an empty output path" in {
    val workload = mkWorkload(confMapTest + ("tpcds-dsdgen-output" -> ""))
    workload.dsdgenOutputPath shouldBe ""
  }

  it should "dsdgenOutputPath with no output" in {
    val workload = mkWorkload(confMapTest - "tpcds-dsdgen-output")
    workload.dsdgenOutputPath shouldBe "tpcds-data/"
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
    val hdfsDataDir = workload.dsdgenOutputPath
    val dstDir = new Path(hdfsDataDir)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    dstFs.delete(dstDir, true)
    deleteLocalDir(workload.dsdgenOutputPath)
  }

  private def genDataTest(tableName: String, numPartitions: Option[Int]): Unit = {
    val workload = mkWorkload

    cleanupOutput(workload)

    implicit val spark = SparkSessionProvider.spark
    implicit val ec = ExecutionContext.fromExecutorService(newFixedThreadPool(numPartitions.getOrElse(1)))
    val (dur, results) = time(workload.genDataWithTiming(Seq(TableOptions(tableName, None, numPartitions, Seq.empty))))
    results should have size 1
    results.head._1 shouldBe tableName
    results.head._2 should be > 0L
    results.head._2 should be <= dur
    val r0 = waitForFutures(Seq(results.head._3))
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
  }

  /*
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

  //  it should "doWorkload" in {
  //    val workload = mkWorkload(confMapTest + ("clean" -> true))
  //    val spark = SparkSessionProvider.spark
  //    workload.doWorkload(None, spark).show(tables.length)
  //  }

  */
  it should "cleanup output when done" in {
    cleanupOutput(mkWorkload)
  }
}
