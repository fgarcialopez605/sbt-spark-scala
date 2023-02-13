package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object TestDeployApp {

  /**
   *
   * 1 - Generamos el assembly JAR correctamente ejecutando la task “assembly” en el shell sbt.
   * Tenemos que haber añadido el plugin en project/plugins.sbt y puesto las dependencias “provided” para Spark.
   *
   * 2 - Start a cluster with one master and 3 workers:
   * docker-compose up —scale spark-worker=3
   *
   * 3 - Conectarse al contenedor del master y abrir un shell
   *
   * $ docker exec -it spark-cluster-spark-master-1 bash
   *
   * 4 - Desde el host local copiar el assembly JAR al contenedor del master
   *
   * $ docker cp SbtExampleProject-assembly-0.1.0-SNAPSHOT.jar spark-cluster-spark-master-1:/
   *
   * 5 - Si tenemos un volumen montado nos basta con mover los datos y el assembly JAR a los directorios locales data y apps del spark-cluster
   *
   * $ cp ./target/scala-2.12/SbtExampleProject-assembly-0.1.0-SNAPSHOT.jar ./spark-cluster/apps
   * $ cp ./src/main/resources/data/movies.json ./spark-cluster/data
   *
   * 6 - Comprobamos que tenemos los datos y el assembly JAR subidos al master:
   *
   * $ ls -ltr /opt/spark-data
   * $ ls -ltr /opt/spark-apps
   *
   * 7 - Ejecutamos un spark-submit en el contenedor del master para enviar el JAR al cluster
   *
   * $ docker exec -it spark-cluster-spark-master-1 ./spark/bin/spark-submit --class part6practical.TestDeployApp --master spark://spark-master:7077 /opt/spark-apps/SbtExampleProject-assembly-0.1.0-SNAPSHOT.jar /opt/spark-data/movies.json /opt/spark-data/GoodComedies.json
   *
   * Nota: Podemos averiguar la URL del master consultando las variables de entorno de SPARK de la siguiente forma:
   * $ docker exec -it spark-cluster-spark-master-1 env | grep SPARK
   *
   * 8 - Verificamos la salida del spark-submit y que se han escrito los datos correctamente:
   *
   * $ docker exec -it spark-cluster-spark-master-1 ls -ltr /opt/spark-data/GoodComedies.json
   *
   */

  def main(args: Array[String]): Unit = {
    /**
      * Movies.json as args(0)
      * GoodComedies.json as args(1)
      *
      * good comedy = genre == Comedy and IMDB > 6.5
      */

    if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      .getOrCreate()

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      col("Title"),
      col("IMDB_Rating").as("Rating"),
      col("Release_Date").as("Release")
    )
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5)
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }

   /*
    * Build a JAR to run a Spark application on the Docker cluster
    *
    *   - project structure -> artifacts, add artifact from "module with dependencies"
    *   - (important) check "copy to the output folder and link to manifest"
    *   - (important) then from the generated folder path, delete so that the folder path ends in src/
    *
    * Build the JAR: Build -> Build Artifacts... -> select the jar -> build
    * Copy the JAR and movies.json to spark-cluster/apps
    * (the apps and data folders are mapped to /opt/spark-apps and /opt/spark-data in the containers)
    *
    *
    * */

  /**
    * How to run the Spark application on the Docker cluster
    *
    * 1. Start the cluster
    *   docker-compose up --scale spark-worker=3
    *
    * 2. Connect to the master node
    *   docker exec -it spark-cluster_spark-master_1 bash
    *
    * 3. Run the spark-submit command
    *   /spark/bin/spark-submit \
    *     --class part6practical.TestDeployApp \
    *     --master spark://(dockerID):7077 \
    *     --deploy-mode client \
    *     --verbose \
    *     --supervise \
    *     spark-essentials.jar /opt/spark-data/movies.json /opt/spark-data/goodMovies
    */
}
