package part3typedatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

/**
  * @author mkarki
  */
object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert DF to DS
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)
}
