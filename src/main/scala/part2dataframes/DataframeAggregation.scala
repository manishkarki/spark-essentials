package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, col, count, countDistinct}

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
  moviesDF.select(count("*").as("genre_count"))

  moviesDF.select(countDistinct("Major_Genre"))
  // approximate count
  moviesDF.select(approx_count_distinct("Major_Genre")).show
}
