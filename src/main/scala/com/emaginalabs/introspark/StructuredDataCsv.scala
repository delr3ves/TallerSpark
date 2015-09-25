package com.emaginalabs.introspark

import com.emaginalabs.introspark.beans.CellTower
import org.apache.commons.cli.{Options, CommandLine}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Francisco de Atilano
 */

object StructuredDataCsv extends SparkLauncherUtils {

  val FILE_OPTION = "file"
  val HEADER = "radio,mcc,net,area,cell,unit,lon,lat,range,samples,changeable,created,updated,averageSignal"

  override def getAppName(): String = "Managing structured data: CSV"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the cell towers data is")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val linesRDD: RDD[String] = spark.textFile(options.getOptionValue(FILE_OPTION))
    val towersRDD: RDD[CellTower] = linesRDD
      .filter(!_.contains(HEADER))
      .map(_.split(",").toList)
      .filter(_.size >= 14)
      .map(values => new CellTower(values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13)))

    towersRDD.foreach(tower => println(tower))
  }

}
