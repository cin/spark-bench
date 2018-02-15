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

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.conf

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TpcDsBaseTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val cwd = sys.props("user.dir")
  private val resourcePath = "/tpcds/queries/query_0.sql"
  private val queryAbsPath = s"$cwd/cli/src/test/resources$resourcePath"

  private val idxToTest = 16
  private val stringAtIndex = "-- start query 2 in stream 0 using template query60.tpl and seed 1952747128"

  private val hdfsDir = s"hdfs://localhost:9000/qgen-${getClass.getSimpleName}${System.currentTimeMillis}"
  private val hdfsFile = s"$hdfsDir/query_0.sql"

  override protected def beforeAll(): Unit = {
    val dstPath = new Path(hdfsFile)
    val fs = dstPath.getFileSystem(conf)
    fs.copyFromLocalFile(new Path(queryAbsPath), dstPath)
  }

  override protected def afterAll(): Unit = {
    val hdfsPath = new Path(hdfsDir)
    FileSystem.get(hdfsPath.toUri, conf).delete(hdfsPath, true)
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

  it should "copy tpcds-kit from HDFS" in {
    val outputDir = TpcDsBase.mkKitDir("hdfs://localhost:9000/tpcds-kit")
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
}
