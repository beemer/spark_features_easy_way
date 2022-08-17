package org.avant.spark3.utils

import org.apache.spark.sql.SparkSession

trait Spark3Initializer {
  implicit val sparkSession : SparkSession = {
    SparkSession.builder()
      .appName("Spark 3 Launcher")
      .master("local[4]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.mb","256")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.extraListeners", "")
      .config("spark.hadoop.hive.exec.dynamic.partition", true)
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("spark.driver.memory", "512m")
      .config("spark.executor.memory", "512m")
      .config("spark.executor.heartbeatInterval", 100000L)
      .config("hive.exec.scratchdir","./hive/scratchdir")
      .config("spark.sql.hive.convertMetastoreParquet", false)
      .enableHiveSupport()
      .getOrCreate()
  }
}
