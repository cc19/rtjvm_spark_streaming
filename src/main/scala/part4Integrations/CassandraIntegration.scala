package part4Integrations

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import common._
import org.apache.spark.sql.cassandra._

object CassandraIntegration {

  val spark = SparkSession.builder()
    .appName("Cassandra Integration")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._
  def writeStreamToCassandraInBatches() = {
    var carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreachBatch {(batch: Dataset[Car], _:Long) =>
        //save this batch to cassandra in a single table write
        batch.select("Name", "Horsepower")
          .write
          .cassandraFormat("cars", "public")  //type enrichment
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  class CarCassandra extends ForeachWriter[Car] {

    /*
    - on every batch, on every partition 'partitionId'
      - on every 'epoch' = chunk of data
          - call the open method
          - for each entry in this chunk, call the process method
          - call the close method either at the end of the chunk or with an error if it was thrown
     */

    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open Connection")
      true
    }

    override def process(car: Car): Unit = {
      connector.withSessionDo { session =>
        session.execute(
          s"""
            |insert into $keyspace.$table("Name", "Horsepower")
            |values ('${car.Name}', ${car.Horsepower.orNull})
            |""".stripMargin
        )
      }
    }

    override def close(errorOrNull: Throwable): Unit = println("Close Connection")
  }

  def writeStreamToCassandra() = {
    var carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreach(new CarCassandra)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToCassandra()
  }

}
