package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * @author mkarki
  */
object SparkSql extends App {
  val spark = SparkSession
    .builder()
    .appName("spark sql prac")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  var carsDF = spark.read
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF
    .select(col("Name"))
    .where(col("Name") === "USA")

  // use spark sql
  carsDF.createOrReplaceTempView("cars")

  spark
    .sql("""
      | SELECT Name FROM cars WHERE origin = 'USA'
    """.stripMargin)
    .show()

  // we can run any SQL statement
  spark.sql("CREATE database rtjvm")
  spark.sql("USE rtjvm")
  val databasesDF = spark.sql("SHOW databases")

  // transfer tables from a DB to spark tables
  val driver: String = "org.postgresql.Driver"
  val url: String = "jdbc:postgresql://localhost:5432/rtjvm"
  val uName: String = "docker"
  val password: String = "docker"

  // reading from a DB postgress, just go to the terminal and enter: docker-compose up
  def readTable(tableName: String) =
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", uName)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()

  def transferTables(tableNames: List[String]) =
    tableNames.foreach(tableName => {
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    })
  val employeesDF = readTable("employees")
  employeesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")

  transferTables(
    List("employees",
         "departments",
         "titles",
         "dept_emp",
         "salaries",
         "dept_manager")
  )

  // read DF from DW
  val empployeesDF2 = spark.read
    .table("employees")
    .show()
}