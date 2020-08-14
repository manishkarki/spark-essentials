package part3typedatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author mkarki
  */
object ComplexTypes extends App {
  val spark = SparkSession
    .builder()
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
    .select(
      col("Title"),
      to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // conversion
    .withColumn("Today", current_date()) // today
    .withColumn("Right_now", current_timestamp()) // now
    .withColumn("movie_age", datediff(col("Today"), col("Actual_Release")) / 365) // diff in days, similarly we also have date_add, date_sub

  moviesWithReleaseDates.select("*")
    .where(col("Actual_Release").isNull)

  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse dates
    */
  // 1 - parse the DF multiple times, then union the small DFs
  //    - ignore the minimum rows which are corrupted

  val stocksDF = spark.read
    .format("csv")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  stocksDF.select(col("symbol"), to_date(col("date"), "MMM dd YYYY"))

  // Structures: groups of columns aggregated in one

  // 1 - with col operators
  moviesDF.select(
    col("title"),
    struct(col("US_Gross"), col("Worldwide_Gross"))
      .as("Revenue")
  ).select(col("title"), col("Revenue").getField("US_Gross").as("US_Revenue"))

  //2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Revenue")
    .selectExpr("Title", "Revenue.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF.select(
    col("Title"),
    split(col("Title"), " |,").as("Title_Words") // an array of strings
  )

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_words")),
    array_contains(col("Title_Words"), "Love")
  )
}
