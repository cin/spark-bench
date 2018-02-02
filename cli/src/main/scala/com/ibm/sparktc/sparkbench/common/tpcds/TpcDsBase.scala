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

import java.io.FileNotFoundException

import scala.io.Source.{fromFile, fromInputStream}
import scala.util.Try

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

  def loadFile(fn: String): Try[Iterator[String]] = {
    Try {
      fromFile(fn).getLines
    }.recover {
      case _: FileNotFoundException =>
        log.warn(s"Failed to load file $fn from filesystem. Trying from resource")
        fromInputStream(getClass.getResourceAsStream(fn)).getLines
    }
  }
}
