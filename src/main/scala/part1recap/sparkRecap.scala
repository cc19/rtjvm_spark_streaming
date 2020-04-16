package part1recap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkRecap {

  //entry point to the spark structured API
  val spark = SparkSession.builder()
    .appName("spark recap")
    .config("spark.master","local")
    .getOrCreate()

  //read a DF
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars/cars.json")

  def main(args: Array[String]): Unit = {
    //showing a DF to the console
    carsDF.show()
    carsDF.printSchema()
  }

  //select
  import spark.implicits._
  carsDF.select(
    col("Name"),
    $"Year" //another column object. Needs spark implicits
  )

}
