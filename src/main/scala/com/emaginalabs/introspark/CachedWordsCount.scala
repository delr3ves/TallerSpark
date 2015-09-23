package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */

object CachedWordsCount extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = "CachedWordsCount"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val fileRDD = spark.textFile(options.getOptionValue(FILE_OPTION))
    val wordsRDD = fileRDD.flatMap(row => {
      println("Found a row")
      row.split(" ")
    })

    println("Counting words again with no cache ....")
    println("Found words:" + wordsRDD.count())
    println("Counting words again with no cache so will show messages again ....")
    Thread.sleep(5000)
    println("Found words:" + wordsRDD.count)

    wordsRDD.cache

    println(s"NOW WITH CACHE will perform operation last time!!!!!")
    println("Found words:" +wordsRDD.count())
    println("Counting words again with cache ....")
    println("Found words:" +wordsRDD.count())
  }

}
