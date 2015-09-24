package com.emaginalabs.introspark

import java.util

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

/**
 * @author Sergio Arroyo - @delr3ves
 */

object ReadFilesWithPlaceholder extends SparkLauncherUtils {

  val FILE_OPTION = "file"

  override def getAppName(): String = getClass.getSimpleName

  override def addSpecificOptions(options: Options): Options = {
    options.addOption(FILE_OPTION, true, "The input file where the words are")
  }

  override def execute(spark: SparkContext, options: CommandLine): Unit = {
    val fileRDD = spark.newAPIHadoopFile(options.getOptionValue(FILE_OPTION),
      classOf[NonSplittableLzoTextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(_._2.toString)

    val wordsRDD = fileRDD.flatMap(_.split(" "))
    val words: Long = wordsRDD.count()
    println(s"Found $words words")
  }

}

import org.apache.hadoop.mapreduce.lib.input.{InvalidInputException, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext}

class NonSplittableLzoTextInputFormat extends TextInputFormat {

  private val log = LoggerFactory.getLogger(getClass)

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    try {
      super.getSplits(job);
    } catch {
      case e: InvalidInputException =>
        log.info("No file match with " + e.getMessage)
        new util.ArrayList[InputSplit]
    }
  }
}