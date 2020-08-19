package part5lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

import scala.io.Source

/**
  * @author mkarki
  */
object RDDs extends App {
  val spark = SparkSession
    .builder()
    .appName("RDDs")
    .config("spark.master", "local")
    .getOrCreate

  // entry point to rdds
  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from Files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(fileName: String) =
    Source
      .fromFile(fileName)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD =
    sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc
    .textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from DF
  val stocksDF = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
//  val stocksDS = stocksDF.as[StockValue]
//  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers")

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD)

  // Transformations
  val msRDD = stocksRDD.filter(_.symbol.equals("MSFT")) // lazy transformation
  val msCount = msRDD.count // eager action

  // counting
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also a lazy transformation

  implicit val stockOrdering =
    Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  // ordering
  val minMS = msRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol) // very expensive operation

  // partitioning
  val repartitionStocksRDD = stocksRDD.repartition(30)

  /*
     Repartitioning is expensive, involves shuffling
     Best practice: partition EARLY, then process that
      Size of a partition 10-100MB
    */

  // coalesce: fewer partition than now
  val coalescedRDD = repartitionStocksRDD.coalesce(15) // does NOT involve shuffling

  /**
    * Exercise
    * 1. Read the movies.json as an RDD
    * 2. show the distinct genres as an RDD
    * 3. Select all the movies in the Drama genre with IMDB rating > 6
    * 4. Show the average rating of movies by genre
    */

  case class Movie(title: String, genre: String, rating: Double)

  val moviesRDD = spark.read
    .json("src/main/resources/data/movies.json")
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    ).where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramaMoviesRDD = moviesRDD.filter(movie => movie.rating > 6 && movie.genre.equals("Drama"))

  /*moviesRDD.toDF().show()
  genresRDD.toDF().show()
  goodDramaMoviesRDD.toDF().show()*/

  case class GenreAvgRating(genre: String, rating: Double)

  //4
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map{
    case(genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show

}
