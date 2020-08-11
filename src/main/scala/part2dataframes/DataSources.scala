package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author mkarki
  */
object DataSources extends App {
  val spark = SparkSession
    .builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    ))

  /**
    * reading a DF:
    * - format
    * - schema (optional) or inferSchema = true
    */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // drop mal formed source, permissive (default)
    .load("src/main/resources/data/cars.json")

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(
      Map(
        "mode" -> "failFast",
        "path" -> "src/main/resources/data/cars.json",
        "inferSchema" -> "true"
      ))
    .load()

  /*
    write DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists
    - path
    - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources.data/cars_dupe") // directory path
    .save() // the option path can be directly sent in as parameter in save

  // Json FLags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed")
    .json("src/main/resources/data/cars.json")

  //CSV Flags
  val stockSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // ignores the first row
    .option("sep", ",") // separator, can be chosen what one needs
    .option("nullValue", "") // empty values can be used as null
    .load("src/main/resources/data/stocks.csv") // or just csv method

  // Parquet ( default storage format for DFs, open sourced compressed binary format)
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars") // by default since we didn't specified any format, this'll get saved as parquet

  // reading from a DB postgress, just go to the terminal and enter: docker-compose up
  val employeeDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()
  // progress: time: 19:40

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy parquet
    * - table public.movies in the postgres db
    */
  val moviesDF = spark.read
    .format("json")
    .load("src/main/resources/data/movies.json")

  println("writing movies.json")

//task 1: tsv
  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("path", "src/main/resources/data/movies-saved")
    .option("sep", "\t")
    .save

  //task 2: save as parquet
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/movies-saved") // default compression is snappy

  //task 3: in postgres
  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save

}
