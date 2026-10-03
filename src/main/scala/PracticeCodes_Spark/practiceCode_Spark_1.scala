package PracticeCodes_Spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object practiceCode_Spark_1 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Testing codes here").getOrCreate()

    println("Hello There")

  }
}
