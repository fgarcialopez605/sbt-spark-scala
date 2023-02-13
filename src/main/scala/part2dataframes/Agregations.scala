package part2dataframes

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Agregations extends App {

  val spark = SparkSession
    .builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count("Major_Genre")) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")

  // counting all
  moviesDF.select(count("*"))  // count all the rows, including nulls

  // counting distinct
  moviesDF.select(countDistinct("Major_Genre"))

  // approximate count
  moviesDF.select(approx_count_distinct("Major_Genre"))

  // min and max
  val minRatingsDF = moviesDF.select(min("IMDB_Rating"))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum("US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg("Rotten_Tomatoes_Rating"))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(mean("Rotten_Tomatoes_Rating"), stddev("Rotten_Tomatoes_Rating"))

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy("Major_Genre")  // includes null
    .count()  // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenre = moviesDF
    .groupBy("Major_Genre")
    .avg("IMDB_Rating")

  val aggsByGenre = moviesDF
    .groupBy("Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  moviesDF.printSchema()

  // 1
  import spark.implicits._
  moviesDF.select(($"US_DVD_Sales" + $"US_Gross" + $"Worldwide_Gross").as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  // 2
  moviesDF.select(countDistinct("Director"))
    .show()

  // 3
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross"))
    .show()

  // 4
  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(desc_nulls_last("Avg_Rating"))
    .show()

}
