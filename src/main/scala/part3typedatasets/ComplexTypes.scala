package part3typedatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author mkarki
  */
object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Complex types")
    .getOrCreate

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //needed for spark 3
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  //Dates
  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // conversion
    .withColumn("Today", current_date()) // today
    .withColumn("Right_now", current_timestamp()) // now
    .withColumn("movie_age", datediff(col("Today"), col("Actual_Release")) / 365) // diff in days, similarly we also have date_add, date_sub

  moviesWithReleaseDates.select("*")
    .where(col("Actual_Release").isNull)
}
