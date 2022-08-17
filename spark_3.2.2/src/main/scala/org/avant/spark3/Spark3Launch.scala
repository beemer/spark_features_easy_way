package org.avant.spark3

import org.avant.spark3.utils.Spark3Initializer

object Spark3Launch extends App with Spark3Initializer{

  val lines = Array("a b c", "a b")
  val wordFreq = sparkSession.sparkContext.parallelize(lines).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
  wordFreq.sortBy(s => -s._2).map(x => (x._2, x._1)).top(10)
  println(s"================\nword frequency as map: ${wordFreq.collectAsMap().toString()}")

}
