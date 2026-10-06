package PracticeCodes_Spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,count}

object practiceCode_Spark_1 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Testing codes here").getOrCreate()

    val data = Seq(
      (101, "Abhi", "ECE", 99),
      (102, "Ajay", "ECE", 95),
      (103, "Akhil", "ECE", 95),
      (104, "Arjun", "ECE", 90),
      (201, "Balu", "CSE", 90),
      (202, "Bharath", "CSE", 90),
      (203, "Bhuvi", "CSE", 90),
      (204, "Bindhu", "CSE", 80),
      (205, "Bittu", "CSE", 70)
    )
    val cols = Seq("StudentID", "StudentName", "DepartmentName", "StudentMarks")
    val df = spark.createDataFrame(data).toDF(cols: _*)

    val finalDF = df.filter(col("StudentMarks") >= 90).groupBy("DepartmentName").agg(
      count("StudentMarks").as("TotalCount")
    )
    finalDF.show(false)

  }
}
