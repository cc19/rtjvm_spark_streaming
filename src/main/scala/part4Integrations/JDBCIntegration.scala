package part4Integrations

import org.apache.spark.sql.{SaveMode,Dataset, SparkSession}
import common._

object JDBCIntegration {

  val spark = SparkSession.builder()
    .appName("JDBC Integration")
    .master("local[2]")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val pwd = "docker"

  import spark.implicits._
  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
    val carsDS = carsDF.as[Car]

    //each executor can control the batch
    //batch is a STATIC Dataset/DataFrame
    carsDS.writeStream
      .foreachBatch { (batch:Dataset[Car], _:Long) =>
        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password",pwd)
          .option("dbtable", "public.cars") //this table will get created while executing the code.
          // But will throw warning if the table already exists while executing the code
          .mode(SaveMode.Append)  //Solution to above issue
          //Other modes: SaveMode.OverWrite, SaveMode.Ignore
          .save()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }

}
