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

  //Grouping
  val countMovieByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes nulls
    .count // select count(*) from movieDF group by movie_genre

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggrByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  /**
    * Exercise
    *
    * 1. Sum up all the profits of All the Movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US Gross income
    * 4. Compute the avg IMDB rating and the avg US gross revenue per direction PER DIRECTOR
    */

}
