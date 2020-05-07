package part6Advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object EventTimeWindowsExercise {

  val spark = SparkSession.builder()
    .appName("Event time window exercise")
    .master("local[2]")
    .getOrCreate()

  /*Exercises
  1. Show the best selling product of everyday + quantity sold
  2. Show the best selling product of every 24 hours, updated every hour
   */
  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readDataFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  //1
  def bestSellingProductDaily() = {
    val purchaseDF = readDataFromFile()

    val orderedProductsDF = purchaseDF
      .groupBy(col("item"), window(col("time"), "1 day").as("day")) //using Tumbling Window
      .agg(sum("quantity").as("totalQty"))
      .orderBy(col("day"), col("totalQty").desc_nulls_last)
        .select(
          col("day").getField("start").as("start"),
          col("day").getField(("end")).as("end"),
          col("item"),
          col("totalQty")
        )

    orderedProductsDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  //2
  def bestSellingProductHourly() = {
    val purchaseDF = readDataFromFile()

    val orderedProductsDF = purchaseDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQty"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField(("end")).as("end"),
        col("item"),
        col("totalQty")
      )
        .orderBy(col("start"), col("totalQty").desc_nulls_last)

    orderedProductsDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //bestSellingProductDaily()
    bestSellingProductHourly()
  }

  //For window functions, window starts at Jan 1, 1970 , 12 AM GMT

}
