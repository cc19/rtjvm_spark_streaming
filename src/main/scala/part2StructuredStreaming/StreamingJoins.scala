package part2StructuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  val guitars = spark.read.option("inferSchema","true").json("src/main/resources/data/guitars")
  val guitarPlayers = spark.read.option("inferSchema","true").json("src/main/resources/data/guitarPlayers")
  val bands = spark.read.option("inferSchema","true").json("src/main/resources/data/bands")

  val bandsSchema = bands.schema
  val guitarPlayersSchema = guitarPlayers.schema

  //joining static DFs
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristBands = guitarPlayers.join(bands, joinCondition, "inner")

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port", 12345)
      .load() //a DF with a single column "value" of type String
      .select(from_json(col("value"),bandsSchema).as("band")) //assuming we get json documents as new input
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    //joining streamed DF with guitar players static DF
    val streamedBandGuitaristsDF = streamedBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamedBandsDF.col("id"), "inner")
    //join happens PER BATCH

    streamedBandGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  //since Spark 2.3 we can join a streamed data with another
  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port", 12345)
      .load() //a DF with a single column "value" of type String
      .select(from_json(col("value"),bandsSchema).as("band")) //assuming we get json documents as new input
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port", 12346) //notice that we will read 2 streams of data from 2 different sockets
      .load() //a DF with a single column "value" of type String
      .select(from_json(col("value"),guitarPlayersSchema).as("guitarPlayers")) //assuming we get json documents as new input
      .selectExpr("guitarPlayers.id as id", "guitarPlayers.name as name", "guitarPlayers.guitars as guitars", "guitarPlayers.band as band")

    //joining a stream with a stream
    val streamedJoin = streamedBandsDF.join(streamedGuitaristsDF, streamedBandsDF.col("id") === streamedGuitaristsDF.col("band"))

    streamedJoin.writeStream
      .format("console")
      .outputMode("append") //only append mode supported for stream joining with stream
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    //joinStreamWithStatic()
    joinStreamWithStream()
  }
}
