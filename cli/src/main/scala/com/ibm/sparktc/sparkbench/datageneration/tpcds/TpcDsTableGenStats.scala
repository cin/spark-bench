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

import scala.concurrent.Future
import org.apache.spark.rdd.RDD

case class TpcDsTableGenStats(table: String, duration: Long)

object TpcDsTableGenStats {
  def apply[T](r: (String, Long, Future[RDD[Seq[TpcDsTableGenResults]]])): TpcDsTableGenStats = {
    new TpcDsTableGenStats(r._1, r._2)
  }
}
