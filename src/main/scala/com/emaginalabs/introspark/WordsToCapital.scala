package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */

object WordsToCapital extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = "WordsToCapital"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val fileRDD = spark.textFile(options.getOptionValue(FILE_OPTION))
    val capitalizedWordsRDD = fileRDD
      .flatMap(_.split(" "))
      .map(_.toUpperCase)
    capitalizedWordsRDD.foreach(println)
  }

}
