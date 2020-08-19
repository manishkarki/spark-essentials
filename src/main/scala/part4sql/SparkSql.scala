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
  def readPostgresTable(tableName: String) =
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", uName)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) =
    tableNames.foreach(tableName => {
      val tableDF = readPostgresTable(tableName)
      tableDF.createOrReplaceTempView(tableName)

      if(shouldWriteToWarehouse) {
        tableDF.write
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
      }
    })
//  val employeesDF = readTable("employees")
 /* employeesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")*/

  transferTables(
    List("employees",
         "departments",
         "titles",
         "dept_emp",
         "salaries",
         "dept_manager")
  )

  // read DF from loaded spark tables
  val employeesDF2 = spark.read
    .table("employees")

  /**
    *  Exercises
    *
    *  1. Read the movies DF and store it as a spark table in the rtjvm database
    *  2. Count how many employees were hired in between Jan 1 2000 and Jan 1 2001
    *  3. Show the average salaries for the employees hired in between those dates, grouped by department
    *  4. Show the name of the best paying department for employees hired in between those dates
    */

  // 1
  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")
  // create the table

  //now save
//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("movies")

  //2
  spark.sql(
    s"""
       | SELECT count(*) FROM employees
       | WHERE hire_date > '2000-01-01' AND hire_date < '2001-01-01'
       |""".stripMargin
  )

  //3
  spark.sql(
    s"""
       | SELECT avg(s.salary), de.dept_no
       | FROM employees e, salaries s, dept_emp de
       | WHERE e.hire_date > '1999-01-01' AND e.hire_date < '2001-01-01'
       |  AND e.emp_no = de.emp_no
       |  AND e.emp_no = s.emp_no
       | GROUP BY de.dept_no
       |""".stripMargin
  )

}
