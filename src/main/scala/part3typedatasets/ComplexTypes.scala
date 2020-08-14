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
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //Dates
  moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy"))
}
