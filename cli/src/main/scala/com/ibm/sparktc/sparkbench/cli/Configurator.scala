package com.ibm.sparktc.sparkbench.cli

import java.util
import scala.collection.JavaConverters._

import com.ibm.sparktc.sparkbench.workload.RunConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}


object Configurator {
//  def apply(path: String) = {
//    val config: Config = ConfigFactory.load(path)
//
//    val workloadObjs: Iterable[ConfigObject] = config.getObjectList("workloads").asScala
//
//    val workloadCfgs: Iterable[Config] = workloadObjs.map(_.toConfig)
//
//    workloadCfgs.map(cfg =>{
//      val name = cfg.getString("name")
//      val runs = cfg.getInt("repeat")
//      val parallel = cfg.getBoolean("parallel")
//      val inputDir = cfg.getStringList("input")
//      val workloadResultsOutputDir = None
//      val outputDir = cfg.getString("output")
//      val workloadSpecific: Map[String, Seq[Any]] = Map.empty
//
//      cfg.entrySet().asScala.toSet
//
//      RunConfig(
//        name = name,
//        runs = runs,
//        parallel = parallel,
//        inputDir = inputDir,
//        workloadResultsOutputDir = workloadResultsOutputDir,
//        outputDir = outputDir,
//        workloadSpecific = workloadSpecific
//      )
//    })
//
//
//
//      val name = work.
//      val runs = 1
//      val parallel = false
//      val inputDir = Seq("")
//      val workloadResultsOutputDir = None
//      val outputDir = ""
//      val workloadSpecific: Map[String, Seq[Any]] = Map.empty
//
//      RunConfig(
//        name = name,
//        runs = runs,
//        parallel = parallel,
//        inputDir = inputDir,
//        workloadResultsOutputDir = workloadResultsOutputDir,
//        outputDir = outputDir,
//        workloadSpecific = workloadSpecific
//      )
//    })
//
//
//    //stubs
//    val name = ""
//    val runs = 1
//    val parallel = false
//    val inputDir = Seq("")
//    val workloadResultsOutputDir = None
//    val outputDir = ""
//    val workloadSpecific: Map[String, Seq[Any]] = Map.empty
//
//      RunConfig(
//        name = name,
//        runs = runs,
//        parallel = parallel,
//        inputDir = inputDir,
//        workloadResultsOutputDir = workloadResultsOutputDir,
//        outputDir = outputDir,
//        workloadSpecific = workloadSpecific
//      )
//  }
}
