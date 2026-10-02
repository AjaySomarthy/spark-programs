package Interviews_Spark.KeywordSearch

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object keywordSearch_In_File {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()

    if (args.length < 2) {
      println("Usage: KeywordSearch C:\\Users\\ajays\\OneDrive\\Desktop\\New_Job_Preperation_2024\\Learning_BigData\\TextInput.txt Ramu")
    sys.exit(1)
    }

    val filePath = args(0)
    val keyword = args(1)

    val textFileRDD = spark.sparkContext.textFile(filePath)
    val keywordExists = textFileRDD.filter(line => line.contains(keyword)).isEmpty()

    if (keywordExists) {
      println(s"The keyword '$keyword' does not exist in the file.")
    }
    else {
      println(s"The keyword '$keyword' exists in the file.")
    }
  }
}
