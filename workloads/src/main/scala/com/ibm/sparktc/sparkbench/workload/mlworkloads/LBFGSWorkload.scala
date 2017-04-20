/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ibm.sparktc.sparkbench.workload.mlworkloads

import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadConfig}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class LBFGSWorkload(conf: WorkloadConfig, spark: SparkSession) extends Workload(conf, spark){


    override def reconcileSchema(df: DataFrame): DataFrame = {
        df
    }

    override def doWorkload(df: DataFrame, spark: SparkSession): DataFrame = {
        val (loadtime, data) = loadToCache(df, spark) // necessary to time this?
        val (trainTime, model) = train(spark)
        val (testTime, _) = test(spark) // necessary?
        val (saveTime, _) = conf.workloadResultsOutputDir match {
            case Some(s) => save(spark)
            case _ => (null, Unit)
        }

        val schema = StructType(
            List(
                StructField("name", StringType, nullable = false),
                StructField("timestamp", LongType, nullable = false),
                StructField("load", LongType, nullable = true),
                StructField("train", LongType, nullable = true),
                StructField("test", LongType, nullable = true),
                StructField("save", LongType, nullable = true)
            )
        )

        val timeList = spark.sparkContext.parallelize(Seq(Row("linear-regression", System.currentTimeMillis(), loadtime, trainTime, testTime, saveTime)))
        println(timeList.first())

        spark.createDataFrame(timeList, schema)
    }

    def loadToCache(df: DataFrame, spark: SparkSession): (Long, Unit) = {
        (null, null)
    }

    def train(spark: SparkSession): (Long, Unit) = {
        (null, null)
    }

    //Within Sum of Squared Errors
    def test(spark: SparkSession): (Long, Unit) = {
        (null, null)
    }

    def save(spark: SparkSession): (Long, Unit) = {
        (null, Unit)
    }


    // Load and parse data
    val data: RDD[String]  = spark.sparkContext.textFile(conf.inputDir)

    val parsedData: RDD[LabeledPoint]  = data.map(LabeledPoint.parse)
    parsedData.cache()

    // Building the model
    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegression.train(parsedData, numIterations, stepSize)

    LinearRegression

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
    val sameModel = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")

}
