package part6Advanced

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration._

object Watermarks {

  val spark = SparkSession.builder()
    .appName("Watermarks")
    .master("local[2]")
    .getOrCreate()

  def debugQuery(query: StreamingQuery) = {
    new Thread(() => {
      (1 to 100).foreach{i =>
        Thread.sleep(1000)
        val queryEventTime =
          if(query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString  //every second we are pulling the max time to show in the console in a separate thread

        println(s"$i: $queryEventTime")
      }
      }
    ).start() //starting the thread that is responsible for debugging the streaming query
  }

  //3000 ms, color = blue
  import spark.implicits._
  def testWatermark() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map{line=>
        val tokens = line.split(",")
        val timeStamp = new Timestamp(tokens(0).toLong)
        val color = tokens(1)

        (timeStamp, color)
      }
      .toDF("created", "color")

    val watermarkDF = dataDF
      .withWatermark("created", "2 seconds")  //adding a 2 seconds watermark
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    /* 2 second watermark means-
    - a window will only be considered until the watermark surpasses the window end
    - an element/ a row/ a record will be considered if AFTER the watermark
     */

    val query = watermarkDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds)) //every 2 seconds a batch will be triggered
      .start()

    debugQuery(query)

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    testWatermark()
  }

  //creating another object whcih will programmatically open a socket and send some data through it
  object dataSender {
    val serverSocket = new ServerSocket(12345)
    val socket = serverSocket.accept()  //blocking call
    val printer = new PrintStream(socket.getOutputStream)

    println("socket accepted")

    def example1() = {
      Thread.sleep(7000)
      printer.println("7000, blue")
      Thread.sleep(1000)
      printer.println("8000, green")
      Thread.sleep(4000)
      printer.println("14000, blue")
      Thread.sleep(1000)
      printer.println("9000, red")  //discarded coz older than the watermark
      Thread.sleep(3000)
      printer.println("15000, red")
      printer.println("8000, blue") //discarded coz older than the watermark
      Thread.sleep(1000)
      printer.println("13000, green")
      Thread.sleep(500)
      printer.println("21000, green")
      Thread.sleep(3000)
      printer.println("4000, purple") //expect to be dropped - discarded coz older than the watermark
      Thread.sleep(2000)
      printer.println("17000, green")
    }

    def main(args: Array[String]): Unit = {
      example1()
    }
  }

}
