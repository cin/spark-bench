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

case class TableRowCounts(tableName: String, rowSz: Int, oneGB: Long, oneTB: Long, threeTB: Long, tenTB: Long, thirtyTB: Long, oneHundredTB: Long)

object TableRowCounts {
  // scalastyle:off magic.number
  private[tpcds] val supportedScales = Seq(
    1,      //   1GB
    1000,   //   1TB
    3000,   //   3TB
    10000,  //  10TB
    30000,  //  30TB
    100000  // 100TB
  )

  private val counts = Seq(
    TableRowCounts("call_center", 305, 6L, 42L, 48L, 54L, 60L, 60L),
    TableRowCounts("catalog_page", 139, 11718L, 30000L, 36000L, 40000L, 46000L, 50000L),
    TableRowCounts("catalog_returns", 166, 144067L, 143996756L, 432018033L, 1440033112L, 4319925093L, 14400175879L),
    TableRowCounts("catalog_sales", 226, 1441548L, 1439980416L, 4320078880L, 14399964710L, 43200404822L, 143999334399L),
    TableRowCounts("customer", 132, 100000L, 12000000L, 30000000L, 65000000L, 80000000L, 100000000L),
    TableRowCounts("customer_address", 110, 50000L, 6000000L, 15000000L, 32500000L, 40000000L, 50000000L),
    TableRowCounts("customer_demographics", 42, 1920800L, 1920800L, 1920800L, 1920800L, 1920800L, 1920800L),
    TableRowCounts("date_dim", 141, 73049L, 73049L, 73049L, 73049L, 73049L, 73049L),
    TableRowCounts("household_demographics", 21, 7200L, 7200L, 7200L, 7200L, 7200L, 7200L),
    TableRowCounts("income_band", 16, 20L, 20L, 20L, 20L, 20L, 20L),
    TableRowCounts("inventory", 16, 11745000L, 783000000L, 1033560000L, 1311525000L, 1627857000L, 1965337830L),
    TableRowCounts("item", 281, 18000L, 300000L, 360000L, 402000L, 462000L, 502000L),
    TableRowCounts("promotion", 124, 300L, 1500L, 1800L, 2000L, 2300L, 2500L),
    TableRowCounts("reason", 38, 35L, 65L, 67L, 70L, 72L, 75L),
    TableRowCounts("ship_mode", 56, 20L, 20L, 20L, 20L, 20L, 20L),
    TableRowCounts("store", 263, 12L, 1002L, 1350L, 1500L, 1704L, 1902L),
    TableRowCounts("store_returns", 134, 287514L, 287999764L, 863989652L, 2879970104L, 8639952111L, 28800018820L),
    TableRowCounts("store_sales", 164, 2880404L, 2879987999L, 8639936081L, 28799983563L, 86399341874L, 287997818084L),
    TableRowCounts("time_dim", 59, 86400L, 86400L, 86400L, 86400L, 86400L, 86400L),
    TableRowCounts("warehouse", 117, 5L, 20L, 22L, 25L, 27L, 30L),
    TableRowCounts("web_page", 96, 60L, 3000L, 3600L, 4002L, 4602L, 5004L),
    TableRowCounts("web_returns", 162, 71763L, 71997522L, 216003761L, 720020485L, 2160007345L, 7199904459L),
    TableRowCounts("web_sales", 226, 719384L, 720000376L, 2159968881L, 7199963324L, 21600036511L, 71999670164L),
    TableRowCounts("web_site", 292, 30L, 54L, 66L, 78L, 84L, 96L)
  )
  // scalastyle:on magic.number

  def getCounts(tableName: String): Option[TableRowCounts] = {
    counts.find(_.tableName == tableName)
  }

  def getCount(tableName: String, scale: Int): Long = getCounts(tableName) match {
    case Some(c) =>
      if (supportedScales.contains(scale)) {
        if (scale == 1) c.oneGB
        else if (scale == 1000) c.oneTB
        else if (scale == 3000) c.threeTB
        else if (scale == 10000) c.tenTB
        else if (scale == 30000) c.thirtyTB
        else if (scale == 100000) c.oneHundredTB
        else 0L
      } else throw new RuntimeException(s"Unsupported scale: $scale")
    case _ => throw new RuntimeException(s"Tablename not found: $tableName")
  }
}
