package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */

object WordsLengthAverage extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = getClass.getSimpleName

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val fileRDD = spark.textFile(options.getOptionValue(FILE_OPTION))
    val lengthByWordRDD = fileRDD
      .flatMap(_.split(" "))
      .map(_.length)

    val mean = lengthByWordRDD.mean
    println(s"The mean of lengths: $mean")
  }

}
