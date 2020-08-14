package part3typedatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author mkarki
  */
object CommonTypes extends App {
  val spark = SparkSession.builder
    .config("spark.master", "local")
    .appName("Common Spark Types")
    .getOrCreate

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF
    .select(col("title"), col("Major_Genre"), lit(47).as("plain_value"))

  //Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  val moviesWithGoodnessFlag =
    moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  val dramaMoviesWithGoodnessFilter =
    moviesWithGoodnessFlag.where(dramaFilter and col("good_movie"))

  //negations
  moviesWithGoodnessFlag.where(not(col("good_movie")))

  //Numbers

  //math operators
  val moviesAvgRatingDF = moviesDF.select(
    col("title"),
    (col("IMDB_RATING") + col("Rotten_Tomatoes_Rating") / 10) / 2
  )

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) /* corr is an action */

  //String
  val carsDF = spark.read
    .json("src/main/resources/data/cars.json")
  //capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vWDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vWDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )
}
