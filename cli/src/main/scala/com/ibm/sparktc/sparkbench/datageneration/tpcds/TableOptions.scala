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

import org.json4s._, native.JsonMethods._

object TableOptions {
  private implicit val formats = DefaultFormats
  def apply(m: Map[String, Any]): Option[Seq[TableOptions]] = m.get("table-options") match {
    case Some(jsonTables: String) =>
      val tables = parse(jsonTables).extract[Seq[TableOptions]]
      if (tables.nonEmpty) Some(tables)
      else None
    case _ => throw new Exception(s"tables configuration item must be present in ${TpcDsDataGen.name} workload")
  }
}

case class TableOptions(name: String, partitions: Option[Int], partitionColumns: Option[Seq[String]])
