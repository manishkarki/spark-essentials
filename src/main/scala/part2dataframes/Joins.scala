package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

/**
  * @author mkarki
  */
object Joins extends App {

  val spark = SparkSession
    .builder
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate

  val guitarsDF = spark.read
    .json("src/main/resources/data/guitars.json")

  val guitaristDF = spark.read
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .json("src/main/resources/data/bands.json")

  // joins
  // inner join
  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristDF.join(bandsDF, joinCondition) // default is inner join

  //outer joins
  //left outer = everything in the inner + all the rows in the LEFT table, with nulls in where data is missing
  guitaristDF.join(bandsDF, joinCondition, "left_outer")

  //right outer = everything in the inner + all the rows in the RIGHT table, with nulls in where data is missing
  guitaristDF.join(bandsDF, joinCondition, "right_outer")

  // semi-joins = everything in the LEFT DF for which there is a row in the right DF satisfying the condition
  guitaristDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = everything in the LEFT DF for which there is NO row in the right DF satisfying the condition
  guitaristDF.join(bandsDF, joinCondition, "left_anti")

  // things to bear in mind
  // guitaristBandsDF.select("id", "band").show // this'll crash

  //option 1 - rename the column on which we're joining
  guitaristDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  //option 2 - drop the dupe column
  guitaristBandsDF.drop(bandsDF.col("id"))

  //option 3- rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "band_id")
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("band_id"))

  //using complex types
  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

}
