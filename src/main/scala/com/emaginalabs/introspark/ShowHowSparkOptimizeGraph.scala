package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */

object ShowHowSparkOptimizeGraph extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = getClass.getName

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val fileRDD = spark.textFile(options.getOptionValue(FILE_OPTION))
    val wordsRDD = fileRDD.flatMap(_.split(" "))
      .map((x) => {
      println(x)
      x.toUpperCase
    })
      .map((x) => {
      println(x.length)
      x.length
    })
    val words: Long = wordsRDD.collect.size
    println(s"Found $words words")
  }

}
