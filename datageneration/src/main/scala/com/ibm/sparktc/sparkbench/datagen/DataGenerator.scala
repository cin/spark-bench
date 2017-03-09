package com.ibm.sparktc.sparkbench.datagen

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DataGenerator(conf: DataGenerationConf) {

  def createSparkContext(): SparkSession = {
    SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
  }

  def generateDataSet(spark: SparkSession): DataFrame

  def saveAsCSV(data: DataFrame): Unit = {
  }

  def writeToDisk(data: DataFrame): Unit = {
    conf.outputFormat match {
      case "csv" => data.write.csv(conf.outputDir)
      case _ => new Exception("unrecognized save format")
    }
  }

  def run(): Unit = {
    val spark = createSparkContext()
    val data = generateDataSet(spark)
    writeToDisk(data)
  }
}
