package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @author Francisco de Atilano
 */

object SparkSqlJsonWithoutSchema extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = "Managing structured data with Spark SQL: JSON with schema"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the cell towers data is")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val sparkSql = new SQLContext(spark)

    val towersDataFrame = sparkSql.read.json(options.getOptionValue(FILE_OPTION))
    towersDataFrame.printSchema()

    towersDataFrame.registerTempTable("towers")
    val filteredTowers = sparkSql.sql("SELECT radio,cell FROM towers WHERE area = 801 AND range > 300")
    filteredTowers.groupBy("radio").count().show()

  }

}
