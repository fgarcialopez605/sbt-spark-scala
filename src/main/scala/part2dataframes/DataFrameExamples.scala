package part2dataframes

import org.apache.spark.sql.SparkSession

object DataFrameExamples extends App {


  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples.com")
    .getOrCreate()

  import spark.implicits._

  val columns = Seq("language", "users_count")
  val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))


  //From Data (USING createDataFrame)
  var dfFromData2 = spark.createDataFrame(data).toDF(columns: _*)
  dfFromData2.printSchema()


}
