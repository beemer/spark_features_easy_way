package org.avant.spark3.utils

import org.apache.spark.sql.Row

object SimpleTestData {
  val flatData = Seq(("Gregory Peck", "Action", "USA", 100000, 58, 1000000, 100),
    ("George Clooney", "Drama", "USA", 70000, 56, 2000000, 50),
    ("Richard Burton", "Action", "UK", 90000, 60, 1300000, 30),
    ("Alec Guinness", "Action", "UK", 80000, 60, 1200000, 20),
    ("Client Eastwood", "Action", "USA", 90000, 70, 3000000, 120),
    ("Will Ferrell", "Action", "USA", 200000, 71, 4000000, 220),
    ("Owen Wilson", "Action", "USA", 100000, 40, 3500000, 80),
    ("Jeanne Moreau", "Drama", "FRA", 30000, 89, 1500000,45),
    ("Audrey Tautou", "Drama", "FRA", 70000, 35, 1800000, 12),
    ("Vittorio De Sica", "Drama", "ITA", 40000, 74, 90000, 5)
  )
  val flatDataSchema = Seq("actor", "genre", "residence", "fee_per_movie", "age", "net_worth", "movie_count")

  val nestedData = Seq(
    Row(Row("Gregory", "", "Peck"), List("Drama","Action", "Comedy"), Row("","Los Angeles","CA","USA"),
      100000, 58, 1000000, 100),
    Row(Row("George", "Timothy", "Clooney"), List("Drama", "Action", "Comedy"), Row("", "Lexington", "KY", "USA"),
      70000, 56, 2000000, 50),
    Row(Row("Richard", "", "Burton"), List("Drama", "Action"), Row("", "Port Talbot", "Wales", "UK"),
      90000, 60, 1300000, 30),
    Row(Row("Alec", "", "Guinness"), List("Drama", "Action", "War"), Row("", "Maida Vale", "London", "UK"),
      80000, 60, 1200000, 20),
    Row(Row("Clint", "", "Eastwood"), List("Action"), Row("", "San Francisco", "CA", "USA"),
      90000, 70, 3000000, 120),
    Row(Row("John", "William", "Ferrell"), List("Action", "Comedy"), Row("", "Irvine", "CA", "USA"),
      200000, 71, 4000000, 220),
    Row(Row("Owen", "Cunningham", "Wilson"), List("Action", "Comedy"), Row("", "Dallas", "TX", "USA"),
      100000, 40, 3500000, 80),
    Row(Row("Jeanne", "", "Moreau"), List("Drama", "Comedy"), Row("", "Paris", "Isle of France", "FRA"),
      30000, 89, 1500000, 45),
    Row(Row("Audrey", "", "Tautou"), List("Drama", "Comedy", "Adventure"), Row("", "Beaumont", "Auvergne-Rhône-Alpes", "FRA"),
      70000, 35, 1800000, 12),
    Row(Row("Vittorio", "De", "Sica"), List("Drama"), Row("", "Sora", "Frosinone", "ITA"),
      40000, 74, 90000, 162)
  )
}
