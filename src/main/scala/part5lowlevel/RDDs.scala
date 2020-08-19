package part5lowlevel

import org.apache.spark.sql.SparkSession

/**
  * @author mkarki
  */
object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local")
    .getOrCreate


}
