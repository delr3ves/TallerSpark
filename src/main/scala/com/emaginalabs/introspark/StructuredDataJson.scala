package com.emaginalabs.introspark

import com.emaginalabs.introspark.beans.CellTower
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Francisco de Atilano
 */

object StructuredDataJson extends SparkLauncherUtils {

  val FILE_OPTION = "file"
  
  override def getAppName(): String = "Managing structured data: JSON"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the cell towers data is")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val linesRDD: RDD[String] = spark.textFile(options.getOptionValue(FILE_OPTION))
    val cellTowersRDD: RDD[CellTower] = linesRDD.flatMap(record => {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      try {
        Some(mapper.readValue(record, classOf[CellTower]))
      } catch {
        case e: Exception => None
      }})

    cellTowersRDD.foreach(tower => {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      println(mapper.writeValueAsString(tower))
    })
  }

}
