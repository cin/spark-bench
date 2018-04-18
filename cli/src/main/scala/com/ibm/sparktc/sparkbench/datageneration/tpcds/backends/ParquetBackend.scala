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

package com.ibm.sparktc.sparkbench.datageneration.tpcds.backends

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.tables
import com.ibm.sparktc.sparkbench.datageneration.tpcds.{TableRowCounts, TpcDsDataGen}
import org.apache.spark.sql.SparkSession

import scala.concurrent.{ExecutionContextExecutorService => ExSvc}
import scala.io.Source.fromInputStream


class ParquetBackend()(implicit spark: SparkSession, exSvc: ExSvc, dg: TpcDsDataGen) extends TpcDsBackend {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  private val TPCDSISDAFT = """^\s*create\s+table\s+([a-zA-Z0-9_]+)\s*\(\s*(.*)$""".r
  private val TPCDSISSALES = """^\s*create\s+table\s+[a-z_]+(sales|returns)(.*)$""".r

  private[backends] def mkPartitionStatement(tableName: String): String = {
    dg.tableOptions.find(_.name == tableName) match {
      case Some(to) if to.partitionColumns.nonEmpty => s"PARTITIONED BY(${to.partitionColumns.mkString(", ")})"
      case _ => ""
    }
  }

  /**
    * For some reason, TPC-DS writes out an extra column when writing out validation rows.
    * The best thing about this is that the TPC-DS spec says nothing about this column or
    * what its expect output should be. In simple cases, it is the primary key. In multipart
    * keys, there doesn't seem to be a rhyme or reason to its value.
    *
    * This hack reuses the existing DDL but inserts a dummy column.
    */
  private[backends] def tpcdsIsDaft(qstr: String): String = qstr match {
    case TPCDSISSALES(sr, _) =>
      log.info(s"$sr table encountered.\n$qstr")
      qstr
    case TPCDSISDAFT(tablename, remainingDDL) => s"create table $tablename(dmy int, $remainingDDL"
    case _ =>
      log.error(s"Unable to match regex in validation DDL for create table string\n$qstr")
      qstr
  }

  private def pruneValidationStmts(sqlStmts: Seq[String], validationPhase: Boolean): Seq[String] = {
    if (validationPhase) Seq(sqlStmts.head, tpcdsIsDaft(sqlStmts(1)))
    else sqlStmts
  }

  override protected[backends] def createDatabase(): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS ${dg.dbName} CASCADE")
    spark.sql(s"CREATE DATABASE ${dg.dbName}")
    spark.sql(s"USE ${dg.dbName}")
  }

  /**
    * Function to create a table in spark. It reads the DDL script for each of the
    * tpc-ds table and executes it on Spark.
    */
  override protected[backends] def createTable(tableName: String, validationPhase: Boolean): Unit = {
    log.info(s"Creating table $tableName...")
    val rs = getClass.getResourceAsStream(s"/tpcds/ddl/$tableName.sql")
    val sqlStmts = fromInputStream(rs)
      .mkString
      .stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", if (validationPhase) dg.tpcDsDataValidationOutput else dg.tpcDsDataOutput)
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .replace("${PARTITIONEDBY_STATEMENT}", mkPartitionStatement(tableName))
      .split(";")
    pruneValidationStmts(sqlStmts, validationPhase).foreach { sqlStmt =>
      log.info(sqlStmt)
      spark.sql(sqlStmt)
    }
  }

  override protected[backends] def validateRowCount(table: String): (String, Long) = {
    val expectedRowCount = TableRowCounts.getCount(table, dg.tpcDsScale)
    val res = spark.sql(s"select count(*) from $table").collect
    if (res.isEmpty) throw new RuntimeException(s"Could not query $table's row count")
    else {
      val actualRowCount = res.head.getAs[Long](0)
      if (actualRowCount != expectedRowCount) {
        log.error(s"$table's row counts did not match expected. actual: $actualRowCount, expected: $expectedRowCount")
      }
      (table, actualRowCount)
    }
  }

  override protected[backends] def validateTable(table: String): Unit = {
    createTable(table, validationPhase = true)
    val validationTable = spark.sql(s"select * from ${dg.validationDbName}.${table}_text")
    validationTable.cache()
    val vtCount = validationTable.count()
    val dataTable = spark.sql(s"select * from ${dg.dbName}.$table")
    val pks = dg.tableOptions.find(_.name == table).map(_.primaryKeys).getOrElse(Set.empty).toSeq
    val res = validationTable.join(dataTable, pks)
    val joinCount = res.count()
    if (joinCount != vtCount) log.error(s"Validation failed for $table. joinCount: $joinCount, expected: $vtCount")
    validationTable.unpersist()
  }

  override protected[backends] def validateDatabase(): Unit = {
    if (dg.tpcDsValidate) {
      dg.genData(validationPhase = true)
      spark.sql(s"CREATE DATABASE ${dg.validationDbName}")
      spark.sql(s"USE ${dg.validationDbName}")
      tables.foreach(validateTable)
      spark.sql(s"DROP DATABASE IF EXISTS ${dg.validationDbName} CASCADE")
    }
  }
}