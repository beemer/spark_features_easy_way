package org.avant.spark3

import org.apache.spark.sql.functions.{col, explode_outer}
import org.avant.spark3.utils.SimpleTestData

object Spark3AQEDemo extends App with SimpleTestData{


  withoutAQE()
  withAQE()
  nestedWithoutAQE()
  nestedWithAQE()

  def withoutAQE(): Unit = {
    sparkSession.conf.set("spark.sql.adaptive.enabled",value = false)
    val df1 = readAsDF.groupBy("genre").count()
    println("Without AQE Part Count *********"+df1.rdd.getNumPartitions)
  }

  def withAQE(): Unit = {
    sparkSession.conf.set("spark.sql.adaptive.enabled",value = true)
    val df2 = readAsDF.groupBy("genre").count()
    println("With AQE Part Count *********"+df2.rdd.getNumPartitions)
  }

  def nestedWithoutAQE(): Unit = {
    sparkSession.conf.set("spark.sql.adaptive.enabled", value = false)
    val df1 = readAsDFWithSchema.groupBy("genre").count()
    println("Nested Without AQE Part Count *********" + df1.rdd.getNumPartitions)
  }

  def nestedWithAQE(): Unit = {
    sparkSession.conf.set("spark.sql.adaptive.enabled", value = true)
    val df2 = readAsDFWithSchema.withColumn("genre",explode_outer(col("genre"))).groupBy("genre").count()
    println("Nested With AQE Part Count *********" + df2.rdd.getNumPartitions)
    df2.show()
  }
}
