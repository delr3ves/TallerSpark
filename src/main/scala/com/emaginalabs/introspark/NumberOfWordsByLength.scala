package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Sergio Arroyo - @delr3ves
 */

object NumberOfWordsByLength extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = getClass.getSimpleName

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val fileRDD = spark.textFile(options.getOptionValue(FILE_OPTION))
    val wordsByLengthRDD = fileRDD.flatMap(_.split(" "))
      .map((word) => (word.length, word))
      .groupByKey
      .map((tuple) => (tuple._1, tuple._2.size))

    wordsByLengthRDD.foreach(println)
  }

}
