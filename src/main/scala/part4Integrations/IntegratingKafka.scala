package part4Integrations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common._

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("Kafka Integration")
    .master("local[2]")
    .getOrCreate()

  //Reading data from a kafka stream
  def readFromKafka() = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")  //9092 is the most common kafka server
      .option("subscribe", "chandua")
      .load()

    //kafkaDF.writeStream
    //To display the string value
    kafkaDF.select(col("topic"), expr("cast(value as String) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  //Writing to kafka
  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    //changing the data to kafka understandable format i.e key value pairs
    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    //writing to Kafka
    carsKafkaDF.writeStream
      .format("Kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "chandua")
      .option("checkpointLocation", "checkpoints")  //without checkpoints, the writing to Kafka will fail
      //if you want to rerun the program, you will have to delete the checkpoint directory first (see project explorer)
      .start()
      .awaitTermination()
  }

  /*Exercise:
  Write the whole cars data structure to kafka in json
  Use struct column and to_json function
   */

  def writeCarsToKafka() =
  {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Year"), col("Origin"))).cast("String").as("value")
    )

    carsKafkaDF.writeStream
      .format("Kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "chandua")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit =
  {
    //readFromKafka()
    //writeToKafka()
    writeCarsToKafka()
  }

}














