package DataFrameOperations.AggregationFunctions

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum,avg,min,max,mean,count}

object MinMaxSumAvgMeanCountWithDepartmentWiseDF {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Finding Min Max Sum Avg marks department wise")
      .getOrCreate()

    val cols = Seq("Name","Branch","Marks")
    val data = Seq(
      ("Anusha", "ECE", 85),
      ("Bindhu", "ECE", 95),
      ("Chitra", "ECE", 75),
      ("Divya", "ECE", 98),
      ("Eesha", "CSE", 55),
      ("Fathima", "CSE", 92),
      ("Ganga", "CSE", 35),
      ("Harika", "CSE", 99)
    )
    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.show(false)

    // Approach 1 : Finding Min, Max, Sum, Avg, Mean, Count all in one shot
    val aggDF = df.groupBy("Branch").agg(
      min("Marks").as("Minimum Marks"),
      max("Marks").as("Maximum Marks"),
      sum("Marks").as("Total Marks"),
      avg("Marks").as("Average Marks"),
      mean("Marks").as("Mean Marks"),
      count("Marks").as("Total Count")
    )
    // aggDF.printSchema()
    // aggDF.show(false)

    //  Approach 2 : Finding Min, Max, Sum, Avg, Mean, Count all one by one separately
    val minDF = df.groupBy("Branch").min("Marks")
    // minDF.show(false)
    val maxDF = df.groupBy("Branch").max("Marks")
    // maxDF.show(false)
    val sumDF = df.groupBy("Branch").sum("Marks")
    // sumDF.show(false)
    val avgDF = df.groupBy("Branch").avg("Marks")
    // avgDF.show(false)
    val meanDF = df.groupBy("Branch").mean("Marks")
    // meanDF.show(false)
    val countDF1 = df.groupBy("Branch").count()
    // countDF1.show(false)
             // (or)
    val countDF2 = df.groupBy("Branch").agg(count("Marks"))
    // countDF2.show(false)

    df.createOrReplaceTempView("Students")
    val df1 = spark.sql("select Branch, min(Marks) as MinMarks, max(Marks) as MaxMarks, " +
      "sum(Marks) as TotalMarks, avg(Marks) as AvgMarks, mean(Marks) as MeanMarks, " +
      "count(Marks) as TotalCount from Students group by Branch")
    // df1.printSchema()
    // df1.show(false)

  }
}
