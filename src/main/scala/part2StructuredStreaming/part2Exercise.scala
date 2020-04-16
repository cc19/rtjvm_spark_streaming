package part2StructuredStreaming

import common.{Car, carsSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object part2Exercise {

  val spark = SparkSession.builder()
    .appName("Part 2 Exercise")
    .master("local[2]")
    .getOrCreate()

  /*
  Exercise:
  1. Count how many powerful cars we have in the DS (HP >140)
  2. Average HP for the entire DS (use complete output mode)
  3. Count the cars by origin
   */

  def readCars() = {
    //specifying an encoder explicitly. Encoders needed for DF to DS transformation
    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.*")
      .as[Car](carEncoder)
  }

  def exercise1() = {

    val cars = readCars()
    val highPowerCars = cars.filter(_.Horsepower.getOrElse(0L) > 140)

    highPowerCars.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def exercise2() = {

    val cars = readCars()
    val avgHP = cars.select(avg(col("Horsepower")))

    avgHP.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def exercise3() = {
    val cars = readCars()

    //By using DF API
    val carsByOriginDF = cars.groupBy("Origin").count()

    //By using DS API
    import spark.implicits._
    val carsByOriginDS = cars.groupByKey(_.Origin).count()

    carsByOriginDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
      //exercise1()
      //exercise2()
        exercise3()
  }

}
