package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */

object WordsUnion extends SparkLauncherUtils {

  val FILE_OPTION = "file1"
  val ANOTHER_FILE_OPTION = "file2"

  override def getAppName(): String = getClass.getSimpleName

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
    options.addOption(ANOTHER_FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val file1RDD = spark.textFile(options.getOptionValue(FILE_OPTION))
    val file2RDD = spark.textFile(options.getOptionValue(ANOTHER_FILE_OPTION))

    val unionRDD = file1RDD.union(file2RDD)
    val wordsRDD = unionRDD.flatMap(_.split(" "))
    val words: Long = wordsRDD.count()

    println(s"The mean of lengths: $words")
  }

}
