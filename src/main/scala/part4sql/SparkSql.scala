package part4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * @author mkarki
  */
object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("spark sql prac")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  var carsDF = spark.read
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name"))
    .where(col("Name") === "USA")

  // use spark sql
  carsDF.createOrReplaceTempView("cars")

  spark.sql(
    """
      | SELECT Name FROM cars WHERE origin = 'USA'
    """.stripMargin)
    .show()

  // we can run any SQL statement
  spark.sql("CREATE database rtjvm")
  spark.sql("USE rtjvm")
  val databasesDF = spark.sql("SHOW databases")
}
