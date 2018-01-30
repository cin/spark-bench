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

package com.ibm.sparktc.sparkbench.workload.tpcds

import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.tables
import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class TpcDsWorkloadTest extends FlatSpec with Matchers {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  private val confMapTest: Map[String, Any] = Map(
    "querystream" -> "/tpcds/queries/query_0.sql"
  )

  private def mkWorkload: TpcDsWorkload = mkWorkload(confMapTest)
  private def mkWorkload(confMap: Map[String, Any]): TpcDsWorkload =
    TpcDsWorkload(confMap).asInstanceOf[TpcDsWorkload]

  /*
  private val q1_ansi =
    """select top 100 count(*)
      |from store_sales
      |    ,household_demographics
      |    ,time_dim, store
      |where ss_sold_time_sk = time_dim.t_time_sk
      |    and ss_hdemo_sk = household_demographics.hd_demo_sk
      |    and ss_store_sk = s_store_sk
      |    and time_dim.t_hour = 15
      |    and time_dim.t_minute >= 30
      |    and household_demographics.hd_dep_count = 0
      |    and store.s_store_name = 'ese'
      |order by count(*)
      |;""".stripMargin
  */

  private val q1 =
    """select  count(*)
      |from store_sales
      |    ,household_demographics
      |    ,time_dim, store
      |where ss_sold_time_sk = time_dim.t_time_sk
      |    and ss_hdemo_sk = household_demographics.hd_demo_sk
      |    and ss_store_sk = s_store_sk
      |    and time_dim.t_hour = 15
      |    and time_dim.t_minute >= 30
      |    and household_demographics.hd_dep_count = 6
      |    and store.s_store_name = 'ese'
      |order by count(*)
      | limit 100;""".stripMargin

  private val q29 =
    """with ss as (
      | select
      |          i_item_id,sum(ss_ext_sales_price) total_sales
      | from
      | 	store_sales,
      | 	date_dim,
      |         customer_address,
      |         item
      | where
      |         i_item_id in (select
      |  i_item_id
      |from
      | item
      |where i_category in ('Men'))
      | and     ss_item_sk              = i_item_sk
      | and     ss_sold_date_sk         = d_date_sk
      | and     d_year                  = 2001
      | and     d_moy                   = 8
      | and     ss_addr_sk              = ca_address_sk
      | and     ca_gmt_offset           = -5
      | group by i_item_id),
      | cs as (
      | select
      |          i_item_id,sum(cs_ext_sales_price) total_sales
      | from
      | 	catalog_sales,
      | 	date_dim,
      |         customer_address,
      |         item
      | where
      |         i_item_id               in (select
      |  i_item_id
      |from
      | item
      |where i_category in ('Men'))
      | and     cs_item_sk              = i_item_sk
      | and     cs_sold_date_sk         = d_date_sk
      | and     d_year                  = 2001
      | and     d_moy                   = 8
      | and     cs_bill_addr_sk         = ca_address_sk
      | and     ca_gmt_offset           = -5
      | group by i_item_id),
      | ws as (
      | select
      |          i_item_id,sum(ws_ext_sales_price) total_sales
      | from
      | 	web_sales,
      | 	date_dim,
      |         customer_address,
      |         item
      | where
      |         i_item_id               in (select
      |  i_item_id
      |from
      | item
      |where i_category in ('Men'))
      | and     ws_item_sk              = i_item_sk
      | and     ws_sold_date_sk         = d_date_sk
      | and     d_year                  = 2001
      | and     d_moy                   = 8
      | and     ws_bill_addr_sk         = ca_address_sk
      | and     ca_gmt_offset           = -5
      | group by i_item_id)
      |  select
      |  i_item_id
      |,sum(total_sales) total_sales
      | from  (select * from ss
      |        union all
      |        select * from cs
      |        union all
      |        select * from ws) tmp1
      | group by i_item_id
      | order by i_item_id
      |      ,total_sales
      |  limit 100;""".stripMargin

  private def fixupQueries(queries: Seq[String]): String =
    queries.map(_.replaceAll("""(?)\s+$""", "")).dropRight(1).mkString("\n")

  private var q0queries: Seq[TpcDsQueryInfo] = _

  "TpcDsWorkload" should "extractQueries from a good file" in {
    val workload = mkWorkload
    val queries = workload.extractQueries
    queries should have size 99

    queries.head.queryNum shouldBe 1
    queries.head.streamNum shouldBe 0
    queries.head.queryTemplate shouldBe "query96.tpl"
    val q1act = fixupQueries(queries.head.queries)
    q1act shouldBe q1

    //scalastyle:off magic.number
    queries(28).queryNum shouldBe 29
    queries(28).streamNum shouldBe 0
    queries(28).queryTemplate shouldBe "query60.tpl"
    val q28act = fixupQueries(queries(28).queries)
    q28act shouldBe q29
    //scalastyle:on magic.number

    println(queries(5))
    q0queries = queries
  }

  private def runQueries(
      qi: (TpcDsQueryInfo, Int))(implicit workload: TpcDsWorkload, spark: SparkSession) = {
    val (queryInfo, i) = qi
    val queryStats = try {
      workload.runQuery(queryInfo)
    } catch { case e: Throwable =>
      log.error(s"$i, ${queryInfo.queryNum}, ${queryInfo.queryTemplate}\n${queryInfo.queries.mkString("\n")}", e)
      Seq(TpcDsQueryStats(queryInfo.queryTemplate, 0, 0))
    }
    (queryInfo.queryNum, queryStats)
  }

  // not sure how to test this bc you can't alter the spark.sql.warehouse.dir after the
  // spark session has been created
  ignore should "mkTempTables" in {
    implicit val spark = SparkSessionProvider.spark
//    spark.conf.set("spark.sql.warehouse.dir", "hdfs://localhost:9000/tpcds-warehouse")
    implicit val workload = mkWorkload(confMapTest + ("createtemptables" -> true))
    workload.mkTempTables
  }

  ignore should "run queries" in {
    implicit val spark = SparkSessionProvider.spark
    implicit val workload = mkWorkload

    tables.foreach { t =>
      spark.read.parquet(s"hdfs://localhost:9000/tpcds-warehouse/tpcds.db/$t").createOrReplaceTempView(t)
    }

    val queries = workload.extractQueries
//    val problemQueries = Seq(
//      "query30.tpl", // cannot resolve '`c_last_review_date_sk`'
//      "query2.tpl"   // extraneous input 'select' expecting
//    )

    val queryStats = queries
//      .filterNot  { q => problemQueries.contains(q.queryTemplate) }
      .zipWithIndex
      .map(runQueries)
    queryStats.foreach(println)
  }

  it should "runQuery with q0" in {
    implicit val spark = SparkSessionProvider.spark
    val workload = mkWorkload
    val stats = workload.runQuery(q0queries.head)
  }
}
