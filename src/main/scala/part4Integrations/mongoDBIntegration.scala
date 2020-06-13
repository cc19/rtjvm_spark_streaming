package part4Integrations

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import org.bson.Document

object mongoDBIntegration {

  val spark=SparkSession.builder()
    .appName("MongoDB Integration")
    .master("local[2]")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.myCollection")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.myCollection")
    .getOrCreate()

  val sc = spark.sparkContext

  // Writing a data in mongoDB
  def writeMongo() = {
    val docs =
      """
  {"name": "Bilbo Baggins", "age": 50}
  {"name": "Gandalf", "age": 1000}
  {"name": "Thorin", "age": 195}
  {"name": "Balin", "age": 178}
  {"name": "Kíli", "age": 77}
  {"name": "Dwalin", "age": 169}
  {"name": "Óin", "age": 167}
  {"name": "Glóin", "age": 158}
  {"name": "Fíli", "age": 82}
  {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq

    sc.parallelize(docs.map(Document.parse)).saveToMongoDB()
  }

  //Reading data from mongoDB
  def readFromMongo() = {
    val docs = sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://127.0.0.1:27017/test.myCollection"))) // Uses the ReadConfig
    val docsDF = docs.toDF()
    docsDF.show()   //to display in tabular format
    /* o/p
    +--------------------+----+-------------+
    |                 _id| age|         name|
    +--------------------+----+-------------+
    |[5ec04b0402715746...| 169|       Dwalin|
    |[5ec04b0402715746...| 167|          Óin|
    |[5ec04b0402715746...| 158|        Glóin|
    |[5ec04b0402715746...|  82|         Fíli|
    |[5ec04b0402715746...|null|       Bombur|
    |[5ec04b0402715746...|  50|Bilbo Baggins|
    |[5ec04b0402715746...|1000|      Gandalf|
    |[5ec04b0402715746...| 195|       Thorin|
    |[5ec04b0402715746...| 178|        Balin|
    |[5ec04b0402715746...|  77|         Kíli|
    +--------------------+----+-------------+
     */
    docs.take(5).foreach(println)   //to display in json document format
    /* O/p
    Document{{_id=5ec04b0402715746851864c3, name=Dwalin, age=169}}
    Document{{_id=5ec04b0402715746851864c5, name=Óin, age=167}}
    Document{{_id=5ec04b0402715746851864c8, name=Glóin, age=158}}
    Document{{_id=5ec04b0402715746851864ca, name=Fíli, age=82}}
    Document{{_id=5ec04b0402715746851864cb, name=Bombur}}
     */
  }

  def main(args: Array[String]): Unit = {
    readFromMongo()
  }
}