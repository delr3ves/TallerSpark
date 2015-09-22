package com.emaginalabs.introspark.services

import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */
class WordCounter(spark: SparkContext) {

  def countWords(path: String): Long = {
    val fileRDD = spark.textFile(path)
    val wordsRDD = fileRDD.flatMap(_.split(" "))
    wordsRDD.count()
  }
}
