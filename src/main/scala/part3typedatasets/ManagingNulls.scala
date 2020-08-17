package part3typedatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

/**
  * @author mkarki
  */
object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("managing nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  // select the first non-null value
  moviesDF.select(
    col("title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("first_non_null_rating")
  )
}
