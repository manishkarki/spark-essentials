package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, col}

/**
  * @author mkarki
  */
object DataframeAggregation extends App {
  val spark = SparkSession.builder
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // counting movie by genre
  val genresCountDF = moviesDF.select(count(col("Major_genre")).as("genre_count")) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")
  // count all
  moviesDF.select(count("*").as("genre_count")).show
  genresCountDF.show


}
