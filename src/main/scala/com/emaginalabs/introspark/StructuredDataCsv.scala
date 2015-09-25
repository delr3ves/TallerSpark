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
    val towersRDD: RDD[CellTower] = linesRDD.filter(!_.contains(HEADER)).map(line => {
      val values = line.split(",").toList
      CellTower.tupled(
        values match {
          case List(radio: String, mcc: String, net: String, area: String,cell: String,
                    unit: String, lon: String, lat: String, range: String, samples: String,
                    changeable: String, created: String, updated: String, averageSignal: String)
          =>
               (radio, mcc, net, area, cell,
                 unit, lon, lat, range, samples,
                 changeable, created, updated, averageSignal)
        }
      )
    })

    towersRDD.foreach(tower => println(tower.productIterator.toList))
  }

}
