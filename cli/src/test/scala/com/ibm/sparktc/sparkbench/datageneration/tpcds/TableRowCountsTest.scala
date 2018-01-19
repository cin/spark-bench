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

import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class TableRowCountsTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  private val tableName = "store_sales"
  private val exp = TableRowCounts(
    "store_sales", 164, 2880404L, 2879987999L, 8639936081L, 28799983563L, 86399341874L, 287997818084L) // scalastyle:ignore

  "TableRowCounts" should "getCounts for existing tables" in {
    val res = TableRowCounts.getCounts(tableName)
    res shouldBe defined
    res.foreach { r =>
      r shouldBe exp
    }
  }

  it should "return a none if not found" in {
    TableRowCounts.getCounts("foo") shouldBe None
  }

  it should "get the right count for a table" in {
    TableRowCounts.supportedScales.foreach { scale =>
      val actualCount = TableRowCounts.getCount(tableName, scale)
      val expectedCount = scale match {
        case s if s == 1 => exp.oneGB
        case s if s == 1000 => exp.oneTB
        case s if s == 3000 => exp.threeTB
        case s if s == 10000 => exp.tenTB
        case s if s == 30000 => exp.thirtyTB
        case s if s == 100000 => exp.oneHundredTB
      }
      actualCount shouldBe expectedCount
    }
  }

  it should "throw if tableName not found" in {
    val thrownMsg = the[RuntimeException] thrownBy TableRowCounts.getCount("foo", 0)
    thrownMsg.getMessage shouldBe "Tablename not found: foo"
  }

  it should "throw if scale isn't defined" in {
    val thrownMsg = the[RuntimeException] thrownBy TableRowCounts.getCount(tableName, 0)
    thrownMsg.getMessage shouldBe "Unsupported scale: 0"
  }
}
