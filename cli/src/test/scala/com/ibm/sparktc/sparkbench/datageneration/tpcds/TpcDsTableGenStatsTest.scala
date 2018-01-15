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
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import scala.concurrent.ExecutionContext.Implicits.global

class TpcDsTableGenStatsTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  private implicit val spark = SparkSessionProvider.spark

  private val results = Seq(
    Seq(
      TpcDsTableGenResults("foo1", res = false),
      TpcDsTableGenResults("foo2", res = true)
    )
  )

  "TpcDsTableGenStats" should "be created properly from a tuple" in {
    val rdd = spark.sparkContext.parallelize(results)
    val stats = TpcDsTableGenStats(("foo", 17L, Future(rdd)))
    stats.table shouldBe "foo"
    stats.duration shouldBe 17L
  }
}
