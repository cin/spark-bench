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

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}

import scala.io.BufferedSource
import scala.io.Source.{fromFile, fromInputStream}
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object TpcDsBase {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  val tables = Array(
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site"
  )

  implicit val conf = new Configuration

  def syncCopy(file: File, outputDir: String)(implicit conf: Configuration = conf): Boolean = {
    val dstDir = if (file.isDirectory) new Path(outputDir, file.getName) else new Path(outputDir)
    val dstFs = FileSystem.get(dstDir.toUri, conf)
    // if just copying a file, make sure the directory exists
    // if copying a directory, don't do this
    if (!file.isDirectory && !dstFs.isDirectory(dstDir)) dstFs.mkdirs(dstDir)
    FileUtil.copy(file, dstFs, dstDir, false, conf)
  }

  // TODO: fix this!!! close handles!
  def loadFile(fn: String): Try[Iterator[String]] = {
    Try {
      if (fn.startsWith("hdfs://")) {
        val path = new Path(fn)
        val fs = FileSystem.get(path.toUri, conf)
        val fis = fs.open(path)
        val br = new BufferedSource(fis, fis.available())
        br.getLines()
      } else fromFile(fn).getLines
    }.recover {
      case _: FileNotFoundException =>
        log.warn(s"Failed to load file $fn from filesystem. Trying from resource")
        fromInputStream(getClass.getResourceAsStream(fn)).getLines
    }
  }
}
