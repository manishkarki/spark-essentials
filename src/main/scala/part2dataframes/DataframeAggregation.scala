package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, stddev, sum}

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
  moviesDF.select(approx_count_distinct("Major_Genre"))
  moviesDF.selectExpr("approx_count_distinct(Major_Genre)")

  //sum
  moviesDF.select(sum("US_Gross").as("INCOME_IN_US"))
  moviesDF.selectExpr("sum(US_Gross) as INCOME_IN_US")

  //avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")).as("avg_RT_rating"))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating) as avg_rating_RT")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )
}
