package org.avant.spark3

import org.apache.spark.sql.DataFrame
import org.avant.spark3.utils.SimpleTestData._
import org.avant.spark3.utils.Spark3Initializer

object Spark3AQEDemo extends App with Spark3Initializer{


  withoutAQE()
  withAQE()
  nestedWithoutAQE()
  nestedWithAQE()

  def withoutAQE(): Unit = {
    sparkSession.conf.set("spark.sql.adaptive.enabled",value = false)
    val df1 = flatDataDf.groupBy("genre").count()
    println("Without AQE Part Count *********"+df1.rdd.getNumPartitions)
  }

  def withAQE(): Unit = {
    sparkSession.conf.set("spark.sql.adaptive.enabled",value = true)
    val df2 = flatDataDf.groupBy("genre").count()
    println("With AQE Part Count *********"+df2.rdd.getNumPartitions)
  }

  def nestedWithoutAQE(): Unit = {
    sparkSession.conf.set("spark.sql.adaptive.enabled", value = false)
    val df1 = nestedDataDf.groupBy("genre").count()
    println("Nested Without AQE Part Count *********" + df1.rdd.getNumPartitions)
  }

  def nestedWithAQE(): Unit = {
    sparkSession.conf.set("spark.sql.adaptive.enabled", value = true)
    val df2 = nestedDataDf.groupBy("genre").count()
    println("Nested With AQE Part Count *********" + df2.rdd.getNumPartitions)
  }
}
