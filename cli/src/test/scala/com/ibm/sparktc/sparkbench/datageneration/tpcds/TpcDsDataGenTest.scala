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

class TpcDsDataGenTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  "TpcDsDataGenTest" should "initialize properly given sane inputs" in {
    //      optionallyGet(m, "input"),
    //      optionallyGet(m, "output"),
    //      getOrDefault[String](m, "repo", "https://github.com/SparkTC/tpcds-journey.git"),
    //      getOrDefault[String](m, "journeydir", "tpcds-journey"),
    //      getOrDefault[String](m, "dbname", "tpcds"),
    //      getOrDefault[String](m, "warehouse", "spark-warehouse"),
    //      getOrDefault[Boolean](m, "clean", false),
    //      getOrDefault[String](m, "fsprefix", "hdfs:///"),
    //      TableOptions(m),
    //      optionallyGet(m, "tpcds-kit-dir"),
    //      getOrDefault[Int](m, "tpcds-scale", scaleDefault),
    //      getOrDefault[Int](m, "tpcds-rngseed", rngSeedDefault)
    val confMap: Map[String, Any] = Map(
      "output" -> Some("test-output"),
      "journeydir" -> "test-journey",
      "dbname" -> "test-db",
      "warehouse" -> "test-warehouse",
      "clean" -> false,
      "fsprefix" -> "s3:///"
    )
  }
}
