package com.emaginalabs.introspark

import com.emaginalabs.introspark.services.WordCounter
import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */

object WordCountWithService extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = "TransformationVsAction"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val words = new WordCounter(spark).countWords(options.getOptionValue(FILE_OPTION))
    println(s"Found $words words")
  }

}
