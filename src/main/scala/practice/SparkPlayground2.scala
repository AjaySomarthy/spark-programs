package practice

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// Spark Playground2 for Spark Scala programs
object SparkPlayground2 {
   Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark : SparkSession = SparkSession.builder()
      .master("local[1]").appName("Repartitioning a Data Frame")
      .getOrCreate()

    println("Hello there!")

  }
}
