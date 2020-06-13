import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object schemaRDD extends App {

  val spark = SparkSession.builder()
    .appName("schema RDD")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  //import sc._
  val person = sc.textFile("src/main/resources/data/person.csv")
  val schema = StructType(Array(
    StructField("firstName",StringType,true),
    StructField("lastName",StringType,true),
    StructField("age",IntegerType,true))
  )

  }
