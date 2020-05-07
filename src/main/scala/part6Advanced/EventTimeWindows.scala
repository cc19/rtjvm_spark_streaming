package part6Advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()        //it will return a DF with only one column "value". To break it as per the schema we need:
    .select(from_json(col("value"),onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def aggregatePurchaseBySlidingWindow() = {
    val purchaseDF = readPurchasesFromSocket()

    val windowByDay = purchaseDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) //struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
    //getting start time, end time
        .select(
          col("time").getField("start").as("start"),
          col("time").getField(("end")).as("end"),
          col("totalQuantity")
        )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregatePurchaseBySlidingWindow()
  }

}
