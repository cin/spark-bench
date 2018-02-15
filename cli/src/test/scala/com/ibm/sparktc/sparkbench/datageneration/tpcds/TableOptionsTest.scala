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

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.nio.file.Files.createTempFile

import org.scalatest.{FlatSpec, Matchers}

class TableOptionsTest extends FlatSpec with Matchers {

  private def test(actual: Option[TableOptions], expected: TableOptions): Unit = {
    actual shouldBe defined
    actual.foreach { a =>
      a.name shouldBe expected.name
      a.skipgen shouldBe expected.skipgen
      a.partitions shouldBe expected.partitions
      a.partitionColumns should have size expected.partitionColumns.length
      a.partitionColumns shouldBe expected.partitionColumns
    }
  }

  private def mkExpected = {
    val csPartitions = 10
    val invPartitions = 7
    val itPartitions = 3

    Seq(
      TableOptions("call_center", None, None, Seq.empty),
      TableOptions("catalog_sales", None, Some(csPartitions), Seq("cs_sold_date_sk")),
      TableOptions("catalog_returns", Some(true), None, Seq("cr_returned_date_sk")),
      TableOptions("inventory", None, Some(invPartitions), Seq("inv_date_sk", "test_partition_column")),
      TableOptions("item", None, Some(itPartitions), Seq.empty)
    )
  }

  private def mkTempFile(tableOptions: String): String = {
    val newf = createTempFile("table-options", "json").toFile
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newf)))
    bw.write(tableOptions)
    bw.flush()
    bw.close()
    newf.getCanonicalPath
  }

  "TableOptions" should "initialize properly with sane inputs" in {
    val goodTableOptions =
      """[
        |  {
        |    "name": "call_center"
        |  },
        |  {
        |    "name": "catalog_returns",
        |    "skipgen": true,
        |    "partitionColumns": ["cr_returned_date_sk"]
        |  },
        |  {
        |    "name": "catalog_sales"
        |    "partitions": 10
        |    "partitionColumns": ["cs_sold_date_sk"]
        |  },
        |  {
        |    "name": "inventory"
        |    "partitions": 7
        |    "partitionColumns": ["inv_date_sk", "test_partition_column"]
        |  },
        |  {
        |    "name": "item",
        |    "partitions": 3
        |  }
        |]""".stripMargin

    val confMap: Map[String, Any] = Map(
      "table-options" -> mkTempFile(goodTableOptions)
    )

    val res = TableOptions(confMap)
    mkExpected.foreach { exp =>
      val topt = res.find { _.name == exp.name }
      test(topt, exp)
    }

    val tmp = res.filterNot(_.skipgen.getOrElse(false))
    tmp should have size 4
    tmp.find(_.name == "catalog_returns") shouldBe empty
  }

  it should "throw when not present" in {
    val brokenTableOptions = """[
      |  {
      |    "name": "call_center"
      |  },
      |  {
      |    "name": "catalog_sales"
      |    "partitions": 10
      |    "partitionColumns": ["cs_sold_date_sk"]
      |  },
      |  {
      |    "name": "inventory"
      |    "partitions": 7
      |    "partitionColumns": ["inv_date_sk", "test_partition_column"]
      |  },
      |  {
      |    "name": "item",
      |    "partitions": 3
      |  }
      |]""".stripMargin

    val confMap: Map[String, Any] = Map(
      "table-opt1ons" -> mkTempFile(brokenTableOptions)
    )

    val errMsg = s"table-options configuration item must be present in ${TpcDsDataGen.name} workload"
    val thrownMsg = the[Exception] thrownBy TableOptions(confMap)
    thrownMsg.getMessage shouldBe errMsg
  }
}
