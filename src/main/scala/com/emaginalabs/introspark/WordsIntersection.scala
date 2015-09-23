package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Sergio Arroyo - @delr3ves
 */

object WordsIntersection extends SparkLauncherUtils {

  val FILE_OPTION = "file1"
  val ANOTHER_FILE_OPTION = "file2"

  override def getAppName(): String = getClass.getSimpleName

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
    options.addOption(ANOTHER_FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val words1RDD = splitToWords(spark.textFile(options.getOptionValue(FILE_OPTION)))
    val words2RDD = splitToWords(spark.textFile(options.getOptionValue(ANOTHER_FILE_OPTION)))

    val intersectionRDD = words1RDD.intersection(words2RDD)
    val words: Long = intersectionRDD.count()

    println(s"The intersection: $words")
    intersectionRDD.foreach(println)
  }

  private def splitToWords(input:RDD[String]) = {
    input.flatMap(_.split(" "))
  }

}
