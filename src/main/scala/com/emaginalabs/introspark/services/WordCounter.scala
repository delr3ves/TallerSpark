package com.emaginalabs.introspark.services

import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */
class WordCounter(spark: SparkContext, tokenizer: LineTokenizer = new LineTokenizer) {

  def countWords(path: String): Long = {
    val fileRDD = spark.textFile(path)
    val wordsRDD = fileRDD
      .flatMap(tokenizer.tokenize)
    wordsRDD.count()
  }
}
