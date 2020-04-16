package part2StructuredStreaming

import common._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object StreamingDataFrames {

  val spark=SparkSession.builder()
    .appName("Streaming DF")
    .master("local[2]")   //for allocating 2 threads in the machine
    .getOrCreate()

  //reading data from a socket
  def readFromSocket() = {
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    //we can add as many transformation we want here between reading and consuming a DF
    //transformations
  val shortlines = lines.filter(length(col("value")) <= 5)

    //tell between a static vs streaming DF
    println(shortlines.isStreaming)

    //consuming a DF
    val query = shortlines.writeStream //lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    //wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    //write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
       // Trigger.ProcessingTime(2.seconds)   //every 2 seconds run the query
       // If we enter data for 5 seconds, we will see the data as output in 3 batches, each of 2 seconds
       // Trigger.Once()  //single batch, then terminate
        Trigger.Continuous(2.seconds) //experimental, every 2 seconds create a batch with whatever data you have
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    demoTriggers()
  }



}
