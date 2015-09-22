package com.emaginalabs.introspark.services

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * @author Sergio Arroyo - @delr3ves
 */
class WordCounterTest extends FunSuite with BeforeAndAfter {

  var spark: SparkContext = _
  var wordCounter: WordCounter = _

  before {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")

    spark = new SparkContext(conf)
    wordCounter = new WordCounter(spark)
  }

  after {
    spark.stop
  }

  test("test empty word file should return 0") {
    val words: Long = wordCounter.countWords(getClass.getResource("/emptyFile.txt").getPath)
    assert(0 === words)
  }

  test("test one word file should return 1") {
    val words: Long = wordCounter.countWords(getClass.getResource("/oneWord.txt").getPath)
    assert(1 === words)
  }

  test("test five word file should return 5") {
    val words: Long = wordCounter.countWords(getClass.getResource("/fiveWords.txt").getPath)
    assert(5 === words)
  }

  test("test not found file should throw error") {
    intercept[InvalidInputException] {
      wordCounter.countWords("/notFoundFile.txt")
    }
  }
}
