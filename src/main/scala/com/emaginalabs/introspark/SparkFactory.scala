package com.emaginalabs.introspark

import org.apache.spark.{SparkContext, SparkConf}

/**
 * @author Sergio Arroyo - @delr3ves
 */
class SparkFactory(sparkConfig:SparkConfig) {

  def getSparkContext(): SparkContext = {
    val sparkConf = new SparkConf setAppName(sparkConfig.appName)
    if (sparkConfig isLocal) {
      sparkConf setMaster("local[2]")
    }
    new SparkContext(sparkConf)

  }
}
