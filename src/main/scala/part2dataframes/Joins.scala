package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

/**
  * @author mkarki
  */
object Joins extends App {

  val spark = SparkSession.builder
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
  guitaristDF.join(bandsModDF,
                   guitaristDF.col("band") === bandsModDF.col("band_id"))

  //using complex types
  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
                   expr("array_contains(guitars, guitarId)"))

  /**
    * Exercises
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. Find the job titles of the best paid 10 employees
    */
  val driver: String = "org.postgresql.Driver"
  val url: String = "jdbc:postgresql://localhost:5432/rtjvm"
  val uName: String = "docker"
  val password: String = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", uName)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  //1
  val maxSalaryDF = salariesDF.groupBy("emp_no", "title").agg(max("salary").as("max_salary"))
  val employeesWithMaxSalariesDF = employeesDF.join(maxSalaryDF, "emp_no")
//  employeesWithMaxSalariesDF.show()
  //2
  val empNeverManagersDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti")
//  empNeverManagersDF.show
  //3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesWithMaxSalariesDF.orderBy(col("max_salary").desc).limit(10)

  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")
  bestPaidEmployeesDF.show()
}
