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

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.loadFile
import org.json4s._, native.JsonMethods._

case class TableOptions(
  name: String,
  skipgen: Option[Boolean],
  partitions: Option[Int],
  partitionColumns: Seq[String],
  primaryKeys: Set[String]
)

object TableOptions {
  private implicit val formats = DefaultFormats

  def apply(m: Map[String, Any]): Seq[TableOptions] = m.get("table-options") match {
    case Some(jsonFile: String) => parse(loadFile(jsonFile).get.mkString).extract[Seq[TableOptions]]
    case _ => throw new Exception(s"table-options configuration item must be present in ${TpcDsDataGen.name} workload")
  }
}
