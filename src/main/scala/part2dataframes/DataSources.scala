package part2dataframes

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession
    .builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    StructField("Acceleration", DoubleType) ::
    StructField("Cylinders", LongType) ::
    StructField("Displacement", DoubleType) ::
    StructField("Horsepower", LongType) ::
    StructField("Miles_per_Gallon", DoubleType) ::
    StructField("Name", StringType) ::
    StructField("Origin", StringType) ::
    StructField("Weight_in_lbs", LongType) ::
    StructField("Year", DateType) :: Nil
  )

  // reading a DF
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)  // enforce a schema
    .option("mode","FAILFAST")  // PERMISSIVE (*), DROPMALFORMED, FAILFAST
    .load("src/main/resources/data/cars.json")

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "FAILFAST",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  // writing DFs
  // specify options for bucketing, sorting and partitioning
/*  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)  // ErrorIfExists (*), Append, Overwrite, Ignore
    .save("src/main/resources/data/cars_dupe.json")*/

  // JSON flags
  // https://spark.apache.org/docs/3.1.3/api/java/org/apache/spark/sql/DataFrameReader.html
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "yyyy-mm-dd")  // coupled with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV
  val stocksSchema = StructType(
    StructField("symbol", StringType) ::
    StructField("date", DateType) ::
    StructField("price", DoubleType) :: Nil
  )

  val dfStocks = spark.read
    .options(Map(
      "sep"->",",
      "header"->"true",
      "dateFormat"->"mmm dd yyyy",
      "nullValue"->""
    ))
    .csv("src/main/resources/data/stocks.csv")

  dfStocks.show(false)

  // Parquet (default format)
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable","public.employees")
    .load()

  /*
    Exercise: read the movies DF, then write it as:
    - tabs-separated values file
    - snappy Parquet
    - table public.movies in the Postgres DB
   */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // TSV
  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .option("sep","\t")
    .save("src/main/resources/data/movies.tsv")

  // Parquet
  moviesDF.write.mode(SaveMode.Overwrite).save("src/main/resources/data/movies.parquet")

  // writing to a remote DB
  moviesDF.write
    .format("JDBC")
    .mode(SaveMode.Overwrite)
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable","public.movies")
    .save()




}
