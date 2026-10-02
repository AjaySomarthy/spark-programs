package DataFrameOperations.Transformations.Reshaping.Pivot

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object pivotDfWithCsvFile {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={
    var spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Creating Pivot DF from CSV file").getOrCreate()

    // Finding pivotDF with input CSV file
    val studentsMarksDF = spark.read.option("header", "true").option("Infer", "Schema")
      .csv("C:\\Spark_Sample_Files\\Pivot\\studentsMarks.csv")
    // studentsMarksDF.printSchema()
    // studentsMarksDF.show(false)

    // Here even though I entered integers in Marks column in excel csv file, it is considering them as strings only
    // So I have to manually change the column Marks from String to Integer
    val updatedStudentsMarksDF = studentsMarksDF.withColumn("Marks", col("Marks").cast("Integer"))
    // updatedStudentsMarksDF.printSchema()
    // updatedStudentsMarksDF.show(false)

    val studentsPivotDF = updatedStudentsMarksDF.groupBy("Name").pivot("Subject").sum("Marks")
    // studentsPivotDF.printSchema()
    // studentsPivotDF.show(false)
  }

}
