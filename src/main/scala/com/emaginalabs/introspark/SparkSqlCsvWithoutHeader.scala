package com.emaginalabs.introspark

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @author Francisco de Atilano
 */

object SparkSqlCsvWithoutHeader extends SparkLauncherUtils {

  val FILE_OPTION = "file"
  val SCHEMA = "radio,mcc,net,area,cell,unit,lon,lat,range,samples,changeable,created,updated,averageSignal"
  
  override def getAppName(): String = "Managing structured data with Spark SQL: CSV with no header"

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the cell towers data is")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val sparkSql = new SQLContext(spark)

    val schema = StructType(SCHEMA.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val towersDataFrame = sparkSql.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(schema)
      .load(options.getOptionValue(FILE_OPTION))
    towersDataFrame.printSchema()

    towersDataFrame.registerTempTable("towers")
    val filteredTowers = sparkSql.sql("SELECT radio,cell FROM towers WHERE area = 801")
    filteredTowers.show()

  }

}
