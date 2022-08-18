package org.avant.spark3

import org.apache.spark.sql.DataFrame
import org.avant.spark3.utils.{SimpleTestData, Spark3Initializer}
import org.avant.spark3.utils.SimpleTestData._

object Spark3AQEDemo extends App with Spark3Initializer{

  import sparkSession.implicits._

  val baseDf : DataFrame = {
    flatData.toDF(flatDataSchema : _*)
  }

  withoutAQE()
  withAQE()

  def withoutAQE() = {
    sparkSession.conf.set("spark.sql.adaptive.enabled",false)
    val df1 = baseDf.groupBy("genre").count()
    println("Without AQE Part Count *********"+df1.rdd.getNumPartitions)
  }

  def withAQE() = {
    sparkSession.conf.set("spark.sql.adaptive.enabled",true)
    val df2 = baseDf.groupBy("genre").count()
    println("With AQE Part Count *********"+df2.rdd.getNumPartitions)
  }
}
