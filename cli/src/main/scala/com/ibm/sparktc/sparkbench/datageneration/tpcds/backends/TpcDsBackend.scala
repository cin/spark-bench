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

import scala.concurrent.{ExecutionContextExecutorService => ExSvc}
import org.apache.spark.sql.SparkSession

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.tables
import com.ibm.sparktc.sparkbench.datageneration.tpcds.{TpcDsDataGen, TpcDsTableGenStats}

abstract class TpcDsBackend()(implicit spark: SparkSession, exSvc: ExSvc, dg: TpcDsDataGen) {
  protected def createDatabase(): Unit
  protected def createTable(tableName: String, validationPhase: Boolean): Unit

  protected def validateRowCount(table: String): (String, Long)
  protected def validateTable(table: String): Unit
  protected def validateDatabase(): Unit

  private def addRowCountsToStats(
      stats: Seq[TpcDsTableGenStats],
      rowCounts: Seq[(String, Long)]) = stats.map { r =>
    rowCounts.find(_._1 == r.table) match {
      case Some(rc) => r.copy(rowCount = rc._2)
      case _ => r
    }
  }

  def run(stats: Seq[TpcDsTableGenStats]): Seq[TpcDsTableGenStats] = {
    createDatabase()
    // TODO: optimization possible here.
    // create the tables (convert from CSV to parquet) right after each table has been created
    tables.foreach(createTable(_, validationPhase = false))
    val rowCounts = tables.map(validateRowCount)
    val statsWithRowCounts = addRowCountsToStats(stats, rowCounts)
    validateDatabase()
    statsWithRowCounts
  }
}
