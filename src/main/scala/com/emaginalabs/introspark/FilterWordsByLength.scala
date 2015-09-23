package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */

object FilterWordsByLength extends SparkLauncherUtils {

  val FILE_OPTION = "file"
  val MIN_SIZE_OPTION = "min_size"

  override def getAppName(): String = "Words counter"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
    options.addOption(MIN_SIZE_OPTION, true, "The minimum size of the words to filter")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val size = options.getOptionValue(MIN_SIZE_OPTION) toInt
    val fileRDD = spark.textFile(options.getOptionValue(FILE_OPTION))
    val filteredWordsRDD = fileRDD
      .flatMap(_.split(" "))
      .filter(_.length >= size )
    val words: Long = filteredWordsRDD.count()
    println(s"Found $words words")
  }

}
