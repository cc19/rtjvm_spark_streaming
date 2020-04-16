package part2StructuredStreaming

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._


object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()


  //creating a Dataset from DF

  //implicit reference of encoders
    import spark.implicits._

  def readCars() = {
    //specifying an encoder explicitly. Encoders needed for DF to DS transformation
    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()   //returns a DF with single screen column "value"
      .select(from_json(col("value"), carsSchema).as("car"))  //returns composite column (struct)
      .selectExpr("car.*")  //returns a DF with multiple columns
      .as[Car](carEncoder)  //returns a dataset of type Car
    //For implicit reference of encoders, (carEncoder) this part is not required
  }

  def showCarNames() = {
    val carsDS = readCars()

    //transformations

    //reading the Name of the cars
    val carNamesDF = carsDS.select(col("Name"))   //returns DF

    val carNamesDS = carsDS.map(_.Name)   //returns a DS
    //collection information maintain type info

    carNamesDS.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    showCarNames()
  }
}
