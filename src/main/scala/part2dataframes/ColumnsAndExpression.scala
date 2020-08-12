package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

/**
  * @author mkarki
  */
object ColumnsAndExpression extends App {

  val spark = SparkSession.builder
    .config("spark.master", "local")
    .getOrCreate

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstCol = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstCol)

  // various select methods
  import spark.implicits._
  carsDF.select(
    col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // scala symbol, auto converted to column
    $"Horsepower", // fancier interpolated string
    expr("Origin")
  )

  //select with plain columns
  carsDF.select("Name", "Acceleration")

  // Expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightsInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeight = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightsInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  //selectExpr
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
     "Weight_in_lbs / 2.2"
  )

  // DF processing
  // add a new column
  val carsDFWithKg3 = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  //remove a column
  carsWithColumnRenamed.drop("Cylinders")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression string
  val exprFilter = carsDF.filter(expr("Origin != 'USA'"))
  val exprFilter1 = carsDF.filter("Origin != 'USA'")
  carsDF.where(expr("Origin != 'USA'"))
  //filter chaining
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  // using and instead of two filters
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  // expr
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // union = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", true).json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  //distinct values
  val allCountriesDF = carsDF.select("Origin").distinct

  /**
    * Exercises
    *
    * 1. Read the movies DF, select two columns of your choice
    * 2. Create a new DF by summing up the gross profits (US_Gross, Worldwide_Gross, US_DVD_Sales)
    * 3. Select all comedy movies with IMDB_Rating above 6
    *
    * Use as many versions as possible
    */
}
