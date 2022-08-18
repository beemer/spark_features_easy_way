package org.avant.spark3.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait ImdbData extends DataReader {
  def readAsDF(): DataFrame = {
    val map = Map("header" -> "true", "inferSchema" -> "true", "sep" -> "\t")
    sparkSession.read.options(map).csv("C:\\work\\IdeaProjects\\large_datasets\\imdb\\20220818\\title_basics.tsv")
  }

  def readAsDFWithSchema(): DataFrame = {
    val options = Map("header" -> "true", "sep" -> "\t")

    sparkSession.read.options(options).schema(readSchemaFromClassPath("imdbTitleBasicsTSVSchema.json"))
      .csv("C:\\work\\IdeaProjects\\large_datasets\\imdb\\20220818\\title_basics.tsv").withColumn("genres", split(col("genres"),","))
  }
}
