package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */

object WordsCount extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = "Words counter"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val fileRDD = spark.textFile(options.getOptionValue(FILE_OPTION))
    val wordsRDD = fileRDD.flatMap(_.split(" "))
    val words: Long = wordsRDD.count()
    println(s"Found $words words")
  }

}
