package part3typedatasets

import java.sql.Date

import org.apache.spark.sql.functions.{avg, to_date}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

/**
  * @author mkarki
  */
object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/numbers.csv")

  // convert DF to DS
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type
  // 1 - define the case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                HorsePower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: Date,
                Origin: String
                )
  // 2 - read the DF from the file
  def readDF(fileName: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$fileName")

  // 3 - define an encoder
  import spark.implicits._
  val carsDF = readDF("cars.json").withColumn("Year", to_date($"Year", "yyyy-MM-dd"))
//  implicit val carEncoder = Encoders.product[Car]
  // 4 - convert DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // map, flatMap, fold, reduce for comprehensions
  val carsNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /**
    * Exercise
    *
    * 1. count how many cars we have
    * 2. Powerful cars count, HP > 140
    * 3. Compute the avg HP of entire DS
    */

  //1
  val carsCount = carsDS.count()
  println(carsDS.count())

  // 2
  println(carsDS.filter(_.HorsePower.getOrElse(0L) > 140).count())

  //3
  println(carsDS.map(_.HorsePower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // also use DF functions
  carsDS.select(avg("HorsePower")).show()

}
