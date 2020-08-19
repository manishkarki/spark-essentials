package part5lowlevel

import org.apache.spark.sql.SparkSession

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
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

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
}
