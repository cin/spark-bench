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

package com.ibm.sparktc.sparkbench.datageneration.tpcds.backends

import scala.concurrent.{ExecutionContextExecutorService => ExSvc}
import com.datastax.spark.connector._
import cql.CassandraConnector
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.ibm.sparktc.sparkbench.common.tpcds.TpcDsBase.tables
import com.ibm.sparktc.sparkbench.datageneration.tpcds.{TableRowCounts, TpcDsDataGen}

class CassandraBackend()(implicit spark: SparkSession, exSvc: ExSvc, dg: TpcDsDataGen) extends  TpcDsBackend {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  @transient private var connector: CassandraConnector = _

  private[backends] def setup(): Unit = {
      val warehouseDir = spark.sparkContext.getConf.get("spark.sql.warehouse.dir", "spark-warehouse")
      tables.foreach { t =>
        spark.read.parquet(s"$warehouseDir/${dg.dbName}.db/$t").createOrReplaceTempView(t)
      }
  }

  protected def createDatabase(): Unit = {
    val createKs =
      s"""create keyspace if not exists ${dg.dbName}
         |  WITH replication = {
         |    'class': 'SimpleStrategy',
         |    'replication_factor': 3
         |  };""".stripMargin
    val conf: SparkConf = spark.sparkContext.getConf
    dg.tpcDsSparkCassandraHost.foreach(conf.set("spark.cassandra.connection.host", _))
    dg.tpcDsSparkCassandraUserName.foreach(conf.set("spark.cassandra.auth.username", _))
    dg.tpcDsSparkCassandraPassword.foreach(conf.set("spark.cassandra.auth.password", _))
    connector = CassandraConnector(conf)
    connector.withSessionDo { session =>
      session.execute(createKs)
    }
  }

  def mkKeys(tableName: String): (Option[Seq[String]], Option[Seq[String]]) = {
    dg.tableOptions.find(_.name == tableName) match {
      case Some(to) =>
        if (to.partitionColumns.nonEmpty && to.primaryKeys.nonEmpty)
          (Some(to.partitionColumns), Some(to.primaryKeys.toSeq))
        else if (to.partitionColumns.nonEmpty && to.primaryKeys.isEmpty)
          (Some(to.partitionColumns), None)
        else if (to.partitionColumns.isEmpty && to.primaryKeys.nonEmpty)
          (Some(to.primaryKeys.toSeq), None)
        else (None, None)
      case _ => (None, None)
    }
  }

  protected def createTable(tableName: String, validationPhase: Boolean): Unit = {
    log.info(s"Creating table $tableName...")
    val warehouseDir = spark.sparkContext.getConf.get("spark.sql.warehouse.dir", "spark-warehouse")
    spark.read.parquet(s"$warehouseDir/${dg.dbName}.db/$tableName").createOrReplaceTempView(tableName)
    val df = spark.sql(s"select * from $tableName")
    val (pk, ck) = mkKeys(tableName)
    df.createCassandraTable(dg.dbName, tableName, pk, ck)
    df.write.cassandraFormat(tableName, dg.dbName).save()
  }

  protected def validateRowCount(table: String): (String, Long) = {
    val expectedRowCount = TableRowCounts.getCount(table, dg.tpcDsScale)
    val actualRowCount = spark.sparkContext.cassandraTable(dg.dbName, table).count()
    if (actualRowCount != expectedRowCount) {
      log.error(s"$table's row counts did not match expected. actual: $actualRowCount, expected: $expectedRowCount")
    }
    (table, actualRowCount)
  }

  protected def validateTable(table: String): Unit = {
    log.error("No validateTable method defined yet")
  }

  protected def validateDatabase(): Unit = {
    log.error("No validateDatabase method defined yet")
  }
}
