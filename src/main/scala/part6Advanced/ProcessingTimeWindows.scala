package part6Advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessingTimeWindows {

  val spark=SparkSession.builder()
    .appName("Processing time windows")
    .master("local[2]")
    .getOrCreate()

  def aggregateByProcessingTime() = {
    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(col("value"), current_timestamp().as("Processing_time"))
      .groupBy(window(col("Processing_time"), "10 seconds").as("window"))
      .agg(sum(length(col("value"))).as("charCount")) //counting characters every 10 seconds by processing time
        .select(
          col("window").getField("start").as("Start"),
          col("window").getField("end").as("end"),
          col("charCount")
        )

    linesDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }

}
