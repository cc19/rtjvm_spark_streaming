package part2StructuredStreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    //applying aggregation function
    val lineCount = lines.selectExpr("count(*) as lineCount")

    /*aggregations with distinct not supported because for doing that spark will have to keep track of
    the whole streamed data in the memory
     */
    lineCount.writeStream
      .format("console")
      .outputMode("complete") //append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()

  }

  //numerical aggregations
  def numericalAggregation() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    //changing strings to numbers
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val sumDF = numbers.select(sum(col("number")))

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  //universal numerical aggregations
  def universalNumericalAggregation(aggrFunc: Column => Column) = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    //changing strings to numbers
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val sumDF = numbers.select(aggrFunc(col("number")))

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  //grouping
  def groupNames() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    //counting each occurrence of the "name" value
    val names = lines
      .select(col("value").as("Name"))
      .groupBy (col("Name"))  //returns a RelationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }



  def main(args: Array[String]): Unit = {
    //streamingCount()
    //numericalAggregation()
    //universalNumericalAggregation(stddev) //we can pass any aggregation function here
    groupNames()
    }

}
