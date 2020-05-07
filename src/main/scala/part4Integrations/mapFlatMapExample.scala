package part4Integrations
import org.apache.spark.sql.SparkSession
object mapFlatMapExample extends App {

  val a =List("a", "b", "cat", "dud", "egg")
  val aaa = a.map(aa => aa.toUpperCase)
  //println(aaa)
  //println(aaa.flatten)

  val bb = a.flatMap(aa => aa.toUpperCase)
  //println(bb)


  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master","local")
    .getOrCreate()

  //Entry point for creating RDDs
  val sc=spark.sparkContext
  val rdd = sc.parallelize(Seq("Roses are red", "Violets are blue"))

  println(rdd.map(_.split(" ")).collect.toString)
  println(rdd.flatMap(_.split(" ")).collect.toString)

}
