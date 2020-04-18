package part3DStreams

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamsTransformation {

  val spark=SparkSession.builder()
    .appName("DStreams Transformation")
    .master("local[2]")
    .getOrCreate()

  val ssc=new StreamingContext(spark.sparkContext, Seconds(1))

  def readPeople() = {
    val pplStream = ssc.socketTextStream("localhost", 9999)
    pplStream.map{ line =>
    val tokens = line.split(":")
      Person(
        tokens(0).toInt,  //id
        tokens(1),    //first name
        tokens(2),    //middle name
        tokens(3),    //last name
        tokens(4),    //gender
        Date.valueOf(tokens(5)),    //DOB
        tokens(6),    //ssn/uuid
        tokens(7).toInt   //salary
      )
    }
  }

  //map
  def peopleAges(): DStream[(String, Int)] = {
    val ppl = readPeople()
    ppl.map { person =>
      val ages = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
      (s"${person.firstName} ${person.lastName}", ages)
    }
  }

  //flatMap
  def smallNames() : DStream[String] = {
    val ppl = readPeople()
    ppl.flatMap { person=>
      List(person.firstName, person.middleName)
    }
  }

  //filter
  def highIncomePpl() = {
    val ppl =readPeople()
    ppl.filter(_.salary>80000)
  }

  //count
  def pplCount():DStream[Long] = readPeople().count() //returns number of entries in each batch

  //count by value, PER BATCH
  def countNames():DStream[(String, Long)] = readPeople().map(_.firstName).countByValue()

  /*reduce by key
  - works on DStreams of tuples where the key is the first type and tha values are the second type
  - Values get reduced by the function
  - works per batch
  */
  def countNamesReduce() =
    readPeople()
      .map(_.firstName)
      .map(name => (name,1))
    .reduceByKey(_+_) //can also be written as reduceByKey((a,b) => a+b)

  //foreach RDD (Sample usecase: Saving the file in a different format)
  //write contents of a DStream into a folder
  import spark.implicits._
  def saveToJSON() = readPeople().foreachRDD { rdd =>
    val ds =spark.createDataset(rdd)
    val f = new File("src/main/resources/data/people")
    val nFiles = f.listFiles().length
    val path = s"src/main/resources/data/people/$nFiles.json" //each RDD will be written as a json file in this path

    ds.write.json(path)
  }


  def main(args: Array[String]): Unit = {
    //val pplStream = countNames()
    //pplStream.print()

    saveToJSON()

    ssc.start()
    ssc.awaitTermination()
  }


}
