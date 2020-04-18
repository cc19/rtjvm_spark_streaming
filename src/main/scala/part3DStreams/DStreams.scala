package part3DStreams

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*
  Spark Streaming Context = entry point to the DStreams API
    - needs a spark context
    - duration = batch interval
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))  //means every 1 second the spark engine will pull for new data

  /*
  - define input sources by creating DStreams
  - define transformations on DStreams
  - call an action on the streams
  - start ALL computations with ssc.start()
      - no more computations can be added
  - await termination, or stop the computation
      - you cannot restart the ssc
   */

  def readFromSocket() = {
    //defining input source
    val socketStream = ssc.socketTextStream("localhost", 12345) //returns DStream[String]

    //transformation = lazy
    val wordStream = socketStream.flatMap(line => line.split(" "))  //returns DStream[String]

    //action
    //wordStream.print()

    //If we want to save the words in a file instead of writing it on console
    wordStream.saveAsTextFiles("src/main/resources/data/words")
    /*each folder = RDD = batch
    each file = a partition of the RDD
     */

    //start computations
    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path)    //directory where I will store a new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Apr 1 2000,31.01
          |AAPL,May 1 2000,21
          |AAPL,Jun 1 2000,26.19
          |AAPL,Jul 1 2000,25.41
          |AAPL,Aug 1 2000,30.47
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |AAPL,Dec 1 2000,7.44
          |AAPL,Jan 1 2001,10.81
          |""".stripMargin.trim)

        writer.close()
    }).start()
  }

  def readFromFile() = {
    createNewFile() //operates on another thread
    val socketStream = ssc.textFileStream("src/main/resources/data/stocks") //file system is monitored

    val dateFormat = new SimpleDateFormat("MMM d yyyy")

    val dataStream = socketStream.map{line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val dt = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company,dt, price)
    }

    dataStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
      //readFromSocket()
        readFromFile()
  }

}
