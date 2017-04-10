package com.ibm.sparktc.sparkbench.workload

import com.ibm.sparktc.sparkbench.workload.mlworkloads.KMeansWorkload
import com.ibm.sparktc.sparkbench.utils.SparkFuncs.writeToDisk
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}
import scala.util.{Failure, Success}

object WorkloadKickoff {

  val spark = createSparkContext()

  def apply(conf: WorkloadConfigRoot): Unit = {
    val splitOutConfigs: Seq[WorkloadConfig] = conf.split()
    val results = run(splitOutConfigs, conf.parallel).coalesce(1)
    writeToDisk(data = results, outputDir = conf.outputDir)
  }

  def run(seq: Seq[WorkloadConfig], parallel: Boolean): DataFrame = {
    val dataframes = runWorkloads(seq, parallel)
    joinDataFrames(dataframes)
  }

  // Separating this function for ease of testing
  def runWorkloads(seq: Seq[WorkloadConfig], parallel: Boolean): Seq[DataFrame] = {
    if(parallel) {
      val confSeqPar = seq.par
      confSeqPar.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(seq.size))
      val stuff: ParSeq[DataFrame] = confSeqPar.flatMap( kickoff )
      stuff.seq
    }
    else seq.flatMap( kickoff )
  }


  def createSparkContext(): SparkSession = {
    SparkSession
      .builder()
      .appName("spark-bench workload")
      .getOrCreate()
  }

  def kickoff(conf: WorkloadConfig): Option[DataFrame] = {
    println(s"\t\t\t\t\t\t\t\tTHIS IS THE CONFIG I'M WORKING ON NOW:\n\t\t\t\t\t\t\t\t\t$conf")
    conf.name.toLowerCase match {
      case "kmeans" => Success(new KMeansWorkload(conf, spark).run()).toOption
      case _ => Failure(throw new Exception(s"Unrecognized workload name: ${conf.name}")).toOption
    }
  }

  def joinDataFrames(seq: Seq[DataFrame]): DataFrame = {
    if (seq.length == 1) return seq.head
    // Folding left across this sequence should be fine because each DF should only have 1 row
    // Nevarr Evarr do this to legit dataframes that are all like big and stuff
    seq.foldLeft(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], seq.head.schema))(_.union(_))
  }

}