package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()
  
  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Dates

  val moviesWithReleaseDates = moviesDF
    .select($"Title", to_date($"Release_Date", "dd-MMM-yy").as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff($"Today", $"Actual_Release") / 365) // date_add, date_sub

  moviesWithReleaseDates.select("*").where($"Actual_Release".isNull)

  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse the dates
    */

  // 1 - parse the DF multiple times, then union the small DFs

  // 2
  val stocksDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date", to_date($"date", "MMM dd yyyy"))

  // Structures

  // 1 - with col operators
  moviesDF
    .select($"Title", struct($"US_Gross", $"Worldwide_Gross").as("Profit"))
    .select($"Title", $"Profit".getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays

  val moviesWithWords = moviesDF.select($"Title", split($"Title", " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWords.select(
    $"Title",
    expr("Title_Words[0]"), // indexing
    size($"Title_Words"), // array size
    array_contains($"Title_Words", "Love") // look for value in array
  )

}
