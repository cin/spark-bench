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

package com.ibm.sparktc.sparkbench.common.tpcds

import java.io.File

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import com.holdenkarau.spark.testing.HDFSCluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import TpcDsBase.conf

class TpcDsBaseTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val cwd = sys.props("user.dir")
  private val resourcePath = "/tpcds/queries/query_0.sql"
  private val queryAbsPath = s"$cwd/cli/src/test/resources$resourcePath"

  private val idxToTest = 16
  private val stringAtIndex = "-- start query 2 in stream 0 using template query60.tpl and seed 1952747128"

  private var hdfsDir: String = _
  private var hdfsFile: String = _

  private val kitDir = s"$cwd/cli/src/test/resources/tpcds/${
    sys.props("os.name") match {
      case "Linux" => "linux"
      case _ => "osx"
    }
  }"

  private val hdfsCluster = new HDFSCluster()

  override protected def beforeAll(): Unit = {
    hdfsCluster.startHDFS()
    hdfsDir = s"${hdfsCluster.getNameNodeURI()}/qgen-${getClass.getSimpleName}${System.currentTimeMillis}"
    hdfsFile = s"$hdfsDir/query_0.sql"

    val dstPath = new Path(hdfsFile)
    val fs = dstPath.getFileSystem(conf)
    fs.copyFromLocalFile(new Path(queryAbsPath), dstPath)
  }

  override protected def afterAll(): Unit = {
    hdfsCluster.shutdownHDFS()
  }

  "TpcDsBase" should "read from HDFS" in {
    val lines = TpcDsBase.loadFromHdfs(hdfsFile)
    val expected = TpcDsBase.loadFile(queryAbsPath).get
    lines.toStream shouldBe expected.toStream
  }

  it should "loadFiles from resources" in {
    val lines = TpcDsBase.loadFile(resourcePath).get.toArray
    lines(idxToTest) shouldBe stringAtIndex
    lines should have size 94
  }

  it should "loadFiles from the filesystem" in {
    val lines = TpcDsBase.loadFile(queryAbsPath).get.toArray
    lines(idxToTest) shouldBe stringAtIndex
    lines should have size 94
  }

  it should "loadFiles from HDFS" in {
    val lines = TpcDsBase.loadFile(hdfsFile).get.toArray
    lines(idxToTest) shouldBe stringAtIndex
    lines should have size 94
  }

  it should "upload the tpcds-kit to HDFS" in {
    val src = new File(kitDir)
    val dstPath = new Path(s"${hdfsCluster.getNameNodeURI()}/tpcds-kit")
    FileUtil.copy(src, dstPath.getFileSystem(conf), dstPath, false, conf) shouldBe true
  }

  it should "copy tpcds-kit from HDFS" in {
    val outputDir = TpcDsBase.mkKitDir(s"${hdfsCluster.getNameNodeURI()}/tpcds-kit")
    val dir = new File(outputDir)
    dir.isDirectory shouldBe true
    val dsdgenFile = new File(s"$outputDir/tools/dsdgen")
    dsdgenFile.exists shouldBe true
    dsdgenFile.canExecute shouldBe true
    val dsqgenFile = new File(s"$outputDir/tools/dsqgen")
    dsqgenFile.exists shouldBe true
    dsqgenFile.canExecute shouldBe true
    val idxFile = new File(s"$outputDir/tools/tpcds.idx")
    idxFile.exists shouldBe true
  }

  it should "syncCopy files to HDFS" in {
    val tmpFile = java.nio.file.Files.createTempFile("foo", "tmp")
    val dirString = s"${hdfsCluster.getNameNodeURI()}/foo-tmp"
    val dstDir = new Path(dirString)

    val dstFs = FileSystem.get(dstDir.toUri, conf)
    var createdDir = false
    if (!dstFs.isDirectory(dstDir)) {
      dstFs.mkdirs(dstDir)
      createdDir = true
    }

    TpcDsBase.syncCopy(tmpFile.toFile, dirString) shouldBe true

    if (createdDir) dstFs.delete(dstDir, true)
  }

  // this is mostly yanked from the spark codebase. altered to work without spark utils.
  it should "deleteLocalDir" in {
    val tempDir1 = TpcDsBase.createTempDir(namePrefix = "foo")
    tempDir1.exists() shouldBe true
    TpcDsBase.deleteLocalDir(tempDir1.getAbsolutePath)
    tempDir1.exists() shouldBe false

    val tempDir2 = TpcDsBase.createTempDir(namePrefix = "bar")
    val sourceFile1 = new File(tempDir2, "foo.txt")
    sourceFile1.createNewFile() shouldBe true
    sourceFile1.setLastModified(System.currentTimeMillis) shouldBe true
    sourceFile1.exists() shouldBe true
    TpcDsBase.deleteLocalDir(sourceFile1.getAbsolutePath)
    sourceFile1.exists() shouldBe false

    val tempDir3 = new File(tempDir2, "subdir")
    tempDir3.mkdir() shouldBe true
    val sourceFile2 = new File(tempDir3, "bar.txt")
    sourceFile2.createNewFile() shouldBe true
    sourceFile2.setLastModified(System.currentTimeMillis) shouldBe true
    sourceFile2.exists() shouldBe true
    TpcDsBase.deleteLocalDir(tempDir2.getAbsolutePath)
    tempDir2.exists() shouldBe false
    tempDir3.exists() shouldBe false
    sourceFile2.exists() shouldBe false
  }
}
