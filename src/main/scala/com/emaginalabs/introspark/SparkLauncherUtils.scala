package com.emaginalabs.introspark

import org.apache.commons.cli._
import org.apache.spark.SparkContext

/**
 * @author Sergio Arroyo - @delr3ves
 */
trait SparkLauncherUtils {

  val LOCAL_ARGUMENT: String = "local"

  def main(args: Array[String]) {
    val options = getCmdLine(args)
    val spark = getSparkContext(getAppName(), options)
    execute(spark, options)
    spark.stop()
  }


  def isLocal(cmdLine: CommandLine): Boolean = {
    cmdLine hasOption(LOCAL_ARGUMENT)
  }

  val CONFIG_ARGUMENT: String = "config"

  def getOptions(): Options = {
    val options: Options = new Options
    options addOption(LOCAL_ARGUMENT, false, "executes the topology in local mode")
    options addOption(CONFIG_ARGUMENT, true, "gives the url for config properties")
    options addOption("h", "help", false, "prints this message")
    addSpecificOptions(options)
  }

  def getCmdLine(args: Array[String]): CommandLine = {
    val parser: CommandLineParser = new BasicParser
    val cmdLine: CommandLine = parser.parse(getOptions, args)
    return cmdLine
  }

  def addSpecificOptions(options: Options): Options = {
    options
  }

  def getSparkContext(appName: String, cmdLine:CommandLine): SparkContext = {
    val config = new SparkConfig(appName=appName, isLocal=isLocal(cmdLine))
    new SparkFactory(config) getSparkContext
  }

  def getAppName(): String

  def execute(spark: SparkContext, options: CommandLine)

}
