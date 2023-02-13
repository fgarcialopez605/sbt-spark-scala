package part2dataframes

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Joins extends App {

  val spark = SparkSession
    .builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.json("src/main/resources/data/guitars.json")
  guitarsDF.printSchema()
  guitarsDF.show(false)

  val guitaristsDF = spark.read.json("src/main/resources/data/guitarPlayers.json")
  guitaristsDF.printSchema()
  guitaristsDF.show(false)

  val bandsDF = spark.read.json("src/main/resources/data/bands.json")
  bandsDF.printSchema()
  bandsDF.show(false)

  import spark.implicits._

  // inner joins
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, $"band" === bandsDF("id"))

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, $"band" === bandsDF("id"), "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, $"band" === bandsDF("id"), "right_outer")

  // full outer join = everything in the inner join + all the rows in the BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, $"band" === bandsDF("id"), "outer")

  // semi-joins = everything in the LEFT for which there is a row in the RIGHT satisfying the condition
  guitaristsDF.join(bandsDF, $"band" === bandsDF("id"), "left_semi")

  // anti-join = everything in the LEFT for which there is NO row in the RIGHT satisfying the condition
  guitaristsDF.join(bandsDF, $"band" === bandsDF("id"), "left_anti")

  // things to bear in mind => be aware of ambiguous column names in joined DFs
  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id","band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF("id")).show()

  // option 3 - rename the offending column and keep the data
  val bandModsDF = bandsDF.withColumnRenamed("id","bandId")
  guitaristsDF.join(bandModsDF, $"band" === $"bandId")

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id","guitarId"),expr("array_contains(guitars, guitarId)"))
  guitaristsDF.join(guitarsDF.withColumnRenamed("id","guitarId"), array_contains($"guitars", $"guitarId"))

  /**
   * Exercises
   *
   * 1 - show all employees and their max salary
   * 2 - show all employees who were never managers
   * 3 - find the job titles of the best paid 10 employees in the company
   */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = {
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName)
      .load()
  }

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  employeesDF.show(false)
  salariesDF.show(false)

  // 1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("max_salary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")

  // 2
  employeesDF.join(deptManagersDF,Seq("emp_no"),"left_anti")
    .show(false)

  // 3
  val topTenBestPaidEmployeesDF = employeesSalariesDF.orderBy(desc("max_salary")).limit(10)
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no","title")
    .agg(max("to_date").as("max_to_date"))
    .where("max_to_date = '9999-01-01'")
  topTenBestPaidEmployeesDF.join(mostRecentJobTitlesDF,"emp_no").show(false)

}
