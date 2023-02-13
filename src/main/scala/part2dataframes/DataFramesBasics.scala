package part2dataframes

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

// https://sparkbyexamples.com/spark/different-ways-to-create-a-spark-dataframe/

object DataFramesBasics extends App {

  // creating a Spark Session
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()

  // Create DataFrame from JSON data source
  val firstDF = spark.read.json("src/main/resources/data/cars.json")

  // showing a DF
  firstDF.show(false)
  firstDF.printSchema()

  // get rows
  firstDF.take(10).foreach(println)

  // Create schema manually
  val carsSchema = StructType(
      StructField("Acceleration", DoubleType) ::
      StructField("Cylinders", LongType) ::
      StructField("Displacement", DoubleType) ::
      StructField("Horsepower", LongType) ::
      StructField("Miles_per_Gallon", DoubleType) ::
      StructField("Name", StringType) ::
      StructField("Origin", StringType) ::
      StructField("Weight_in_lbs", LongType) ::
      StructField("Year", StringType) :: Nil
  )

  // Obtain a schema
  val firstDFSchema = firstDF.schema

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  // create DF from tuples
  import spark.implicits._
  val columns = Seq(
    "Acceleration","Cylinders","Displacement","Horsepower","Miles_per_Gallon","Name","Origin","Weight_in_lbs","Year"
  )
  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.5,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars)  // schema auto-inferred
  manualCarsDF.printSchema()

  // create DFs with implicits
  val manualCarsDFWithImplicits = cars.toDF(columns:_*)
  manualCarsDFWithImplicits.printSchema()

  /*
    Exercise:
    1) Create a manual DF describing smartphones
      - make
      - model
      - screen dimension
      - camera megapixels
    2) Read another file from the data folder (movies.json)
      - print its schema
      - count the number of rows
   */

  val smartphones = Seq(
    ("Samsung","Galaxy S10","Android",12),
    ("Apple","iPhone X","iOS",13),
    ("Nokia","3310","THE BEST",0)
  )

  val smartPhonesDF = smartphones.toDF("Make","Model","Platform","CameraMegapixels")
  smartPhonesDF.show()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(s"The movies DF has ${moviesDF.count()} records")



}


