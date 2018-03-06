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

import java.io.{File, FileNotFoundException}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.io.Source.{createBufferedSource, fromFile, fromInputStream}
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}

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
    val dstFs = dstDir.getFileSystem(conf)
    // if just copying a file, make sure the directory exists
    // if copying a directory, don't do this
    if (!file.isDirectory && !dstFs.isDirectory(dstDir)) dstFs.mkdirs(dstDir)
    FileUtil.copy(file, dstFs, dstDir, false, conf)
  }

  private[tpcds] def loadFromHdfs(fn: String)(implicit conf: Configuration = conf): Iterator[String] = {
    val path = new Path(fn)
    val fis = path.getFileSystem(conf).open(path)
    createBufferedSource(fis, close = () => fis.close()).getLines()
  }

  def loadFile(fn: String)(implicit conf: Configuration = conf): Try[Iterator[String]] = {
    Try {
      if (fn.startsWith("hdfs://")) loadFromHdfs(fn)(conf)
      else fromFile(fn).getLines
    }.recover {
      case _: FileNotFoundException =>
        log.warn(s"Failed to load file $fn from filesystem. Trying from resource")
        fromInputStream(getClass.getResourceAsStream(fn)).getLines
    }
  }

  private def updateToolkitPermissions(dir: String)(implicit conf: Configuration = conf): Unit = {
    new File(s"$dir/tools/dsdgen").setExecutable(true)
    new File(s"$dir/tools/dsqgen").setExecutable(true)
  }

  def mkKitDir(kitDir: String)(implicit conf: Configuration = conf): String = kitDir match {
    case kd if kd.startsWith("hdfs://") =>
      val dstDir = createTempDir(namePrefix = "tpcds-kit")
      val dstPath = new Path(dstDir.toURI)
      val dstFs = dstPath.getFileSystem(conf)
      val srcPath = new Path(kd)
      val srcFs = srcPath.getFileSystem(conf)
      if (!FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf))
        throw new RuntimeException(s"Failed to copy tpcds-kit at $kd to ${dstDir.getCanonicalPath}")
      val tmpDir = s"${dstDir.getCanonicalPath}/${kd.split('/').last}"
      updateToolkitPermissions(tmpDir)(conf)
      tmpDir
    case kd => kd
  }

  private val tmpDirs = ArrayBuffer.empty[File]

  def createDirectory(root: String, namePrefix: String = "tpcds"): File = {
    val f = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
    f.mkdirs()
    f.getCanonicalFile
  }

  def createTempDir(
      root: String = sys.props("java.io.tmpdir"),
      namePrefix: String = "tpcds"): File = {
    val dir = createDirectory(root, namePrefix)
    tmpDirs += dir
    dir
  }

  def deleteLocalDir(dirName: String): Unit = Try {
    log.debug(s"Deleting local directory: $dirName")
    val f = new File(dirName)
    if (f.isDirectory) f.listFiles.foreach {
      case ff if ff.isDirectory => deleteLocalDir(ff.getCanonicalPath)
      case ff => ff.delete()
    }
    f.delete()
  }.recover { case e: Throwable => log.error(s"Failed to cleanup $dirName", e) }

  sys.addShutdownHook {
    tmpDirs.foreach { dir =>
      log.info(s"Deleting temp dir: ${dir.getCanonicalPath}")
      deleteLocalDir(dir.getCanonicalPath)
    }
  }
}
