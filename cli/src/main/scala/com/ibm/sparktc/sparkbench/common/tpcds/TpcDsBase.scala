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

import scala.util.Try

object TpcDsBase {
  // TPC-DS table names.
  val tables = Array("call_center", "catalog_sales",
    "customer_demographics", "income_band",
    "promotion", "store", "time_dim", "web_returns",
    "catalog_page", "customer", "date_dim",
    "inventory", "reason", "store_returns", "warehouse",
    "web_sales", "catalog_returns", "customer_address",
    "household_demographics", "item", "ship_mode", "store_sales",
    "web_page", "web_site")
}

abstract class TpcDsBase(dataDir: String) extends Serializable {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  protected val tables: Array[String] = TpcDsBase.tables

  protected val tpcdsRootDir: String = dataDir
  protected val tpcdsDdlDir = s"$tpcdsRootDir/src/ddl/individual"
  protected val tpcdsGenDataDir = s"$tpcdsRootDir/src/data"
  protected val tpcdsQueriesDir = s"$tpcdsRootDir/src/queries"
  protected val tpcdsDatabaseName = "TPCDS2G"

  log.error(s"TPCDS root directory is at $tpcdsRootDir")
  log.error(s"TPCDS ddl scripts directory is at $tpcdsDdlDir")
  log.error(s"TPCDS data directory is at $tpcdsGenDataDir")
  log.error(s"TPCDS queries directory is at $tpcdsQueriesDir")

  // run function for each table in tables array
  protected def forEachTable(tables: Array[String], fn: (String) => Unit): Unit = {
    for (table <- tables) Try(fn(table)).recover {
      case e: Throwable =>
        log.error(s"fn failed on $table", e)
        throw e
    }
  }
}
