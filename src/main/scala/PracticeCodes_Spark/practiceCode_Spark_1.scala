package PracticeCodes_Spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,when}
object practiceCode_Spark_1 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reduce action usage").getOrCreate()

    println("Hello there 123")
  }
}
