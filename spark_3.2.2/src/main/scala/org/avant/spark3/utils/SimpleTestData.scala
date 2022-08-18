package org.avant.spark3.utils

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.io.Source

object SimpleTestData extends Spark3Initializer {

  import sparkSession.implicits._

  val flatDataSchema = Seq("actor", "genre", "residence", "fee_per_movie", "age", "net_worth", "movie_count")

  val flatDataDf: DataFrame = Seq(("Gregory Peck", "Action", "USA", 100000, 58, 1000000, 100),
    ("George Clooney", "Drama", "USA", 70000, 56, 2000000, 50),
    ("Richard Burton", "Action", "UK", 90000, 60, 1300000, 30),
    ("Alec Guinness", "Action", "UK", 80000, 60, 1200000, 20),
    ("Client Eastwood", "Action", "USA", 90000, 70, 3000000, 120),
    ("Will Ferrell", "Action", "USA", 200000, 71, 4000000, 220),
    ("Owen Wilson", "Action", "USA", 100000, 40, 3500000, 80),
    ("Jeanne Moreau", "Drama", "FRA", 30000, 89, 1500000,45),
    ("Audrey Tautou", "Drama", "FRA", 70000, 35, 1800000, 12),
    ("Vittorio De Sica", "Drama", "ITA", 40000, 74, 90000, 5)
  ).toDF(flatDataSchema : _*)


  private val nestedData = Seq(
    Row(Row("Gregory", "", "Peck"), List("Drama","Action", "Comedy"), Row("","Los Angeles","CA","USA"),
      100000L, 58, 1000000L, 100),
    Row(Row("George", "Timothy", "Clooney"), List("Drama", "Action", "Comedy"), Row("", "Lexington", "KY", "USA"),
      70000L, 56, 2000000L, 50),
    Row(Row("Richard", "", "Burton"), List("Drama", "Action"), Row("", "Port Talbot", "Wales", "UK"),
      90000L, 60, 1300000L, 30),
    Row(Row("Alec", "", "Guinness"), List("Drama", "Action", "War"), Row("", "Maida Vale", "London", "UK"),
      80000L, 60, 1200000L, 20),
    Row(Row("Clint", "", "Eastwood"), List("Action"), Row("", "San Francisco", "CA", "USA"),
      90000L, 70, 3000000L, 120),
    Row(Row("John", "William", "Ferrell"), List("Action", "Comedy"), Row("", "Irvine", "CA", "USA"),
      200000L, 71, 4000000L, 220),
    Row(Row("Owen", "Cunningham", "Wilson"), List("Action", "Comedy"), Row("", "Dallas", "TX", "USA"),
      100000L, 40, 3500000L, 80),
    Row(Row("Jeanne", "", "Moreau"), List("Drama", "Comedy"), Row("", "Paris", "Isle of France", "FRA"),
      30000L, 89, 1500000L, 45),
    Row(Row("Audrey", "", "Tautou"), List("Drama", "Comedy", "Adventure"), Row("", "Beaumont", "Auvergne-Rhône-Alpes", "FRA"),
      70000L, 35, 1800000L, 12),
    Row(Row("Vittorio", "De", "Sica"), List("Drama"), Row("", "Sora", "Frosinone", "ITA"),
      40000L, 74, 90000L, 162)
  )

  private val schemaUrl = ClassLoader.getSystemResource("actorBio.json")
  private val schemaSource = Source.fromFile(schemaUrl.getFile).getLines().mkString
  val structSchema = DataType.fromJson(schemaSource).asInstanceOf[StructType]
  val nestedDataDf:DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(nestedData), structSchema)
}
