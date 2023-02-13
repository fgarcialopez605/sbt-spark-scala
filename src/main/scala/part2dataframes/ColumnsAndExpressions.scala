package part2dataframes

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

  val spark = SparkSession
    .builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // Selecting (projecting)
  carsDF.select("Name").show(false)

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to Column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin")  // Expression
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // Expressions
  val weightInKg = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKg.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  val carsWithKgDF = carsDF.withColumn("Weight_in_kg_3", $"Weight_in_lbs" / 2.2)
  // renaming an existing column
  val carsWithColRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsDF.drop("Cylinders", "Displacement")

  // filtering
  val europeansCarsDF = carsDF.filter($"Origin" =!= "USA")
  val europeansCarsDF2 = carsDF.where($"Origin" =!= "USA")
  // filtering with expression strings
  val americansCars = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter($"Origin" === "USA").filter($"Horsepower" > 150)
  val americanPowerfulCarsDF2 = carsDF.filter($"Origin" === "USA" and $"Horsepower" > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' AND Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)  // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show(false)

  /*
   * Exercises:
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movie (US_Gross + Worldwide_Gross + DVD sales)
   * 3. Select all good COMEDY movies with IMDB rating above 6
   */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  moviesDF.select("Title", "Director").show(false)

  moviesDF.printSchema()

  moviesDF.select(
    $"Title",
    $"US_Gross",
    $"Worldwide_Gross",
    ($"US_Gross" + $"Worldwide_Gross").as("Total_Gross"))
    .show(false)

  moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", $"US_Gross" + $"Worldwide_Gross")
    .show(false)

  moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )
    .show(false)

  moviesDF.select("Title", "IMDB_Rating")
    .filter($"Major_Genre" === "Comedy" and $"IMDB_Rating" > 6)
    .orderBy(desc("IMDB_Rating"))
    .show(false)

  moviesDF.select("Title", "IMDB_Rating")
    .where($"Major_Genre" === "Comedy")
    .where($"IMDB_Rating" > 6)
    .orderBy(desc("IMDB_Rating"))
    .show(false)

  moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    .sort(desc("IMDB_Rating"))
    .show(false)


}
