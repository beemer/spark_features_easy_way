package org.avant.spark3

import org.avant.spark3.utils.ImdbData

object Spark3CsvPushdown extends App with ImdbData{

  readTitleBasics()

  def readTitleBasics(): Unit = {
    val titleBasicsDf = readAsDFWithSchema()
    titleBasicsDf.printSchema()
    println("IMDB titleBasicsDf Part Count *********" + titleBasicsDf.rdd.getNumPartitions)
    titleBasicsDf.show(5)
    println(titleBasicsDf.count())
  }

}
