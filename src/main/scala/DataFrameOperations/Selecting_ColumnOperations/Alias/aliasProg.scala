package DataFrameOperations.Selecting_ColumnOperations.Alias

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum,avg,min,max,mean,count}

// operations on a data frame using alias() or as() methods

object aliasProg {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("alias operation on a data frame")
      .getOrCreate()

    val cols = Seq("Name", "Branch", "Marks")
    val data = Seq(
      ("Anusha", "ECE", 85),
      ("Bindhu", "ECE", 95),
      ("Chitra", "ECE", 75),
      ("Divya", "ECE", 98)
    )
    val df = spark.createDataFrame(data).toDF(cols: _*)
    // df.show(false)

    // using as()
    // Approach 1 : Finding Min, Max, Sum, Avg, Mean, Count all in one shot using as(), min(), max(), sum(), avg(), mean(), count() methods
    val aggDF = df.agg(
      min("Marks").as("Minimum Marks"),
      max("Marks").as("Maximum Marks"),
      sum("Marks").as("Total Marks"),
      avg("Marks").as("Average Marks"),
      mean("Marks").as("Mean Marks"),
      count("Marks").as("Total Count")
    )
    // aggDF.printSchema()
    // aggDF.show(false)

    // Approach 2 : Finding Min, Max, Sum, Avg, Mean, Count all one by one separately using as(), min(), max(), sum(), avg(), mean(), count() methods
    val minDF = df.agg(min("Marks")).as("Minimum Marks")
    // minDF.show(false)
    val maxDF = df.agg(max("Marks")).as("Maximum Marks")
    // maxDF.show(false)
    val sumDF = df.agg(sum("Marks")).as("Total Marks")
    // sumDF.show(false)
    val avgDF = df.agg(avg("Marks")).as("Average Marks")
    // avgDF.show(false)
    val meanDF = df.agg(mean("Marks")).as("Mean Marks")
    // meanDF.show(false)
    val countDF = df.agg(count("Marks")).as("Total Count") //Here count acts as an aggregate function
    // countDF.show(false)


    // using alias()
    // Approach 1 : Finding Min, Max, Sum, Avg, Mean, Count all in one shot using as(), min(), max(), sum(), avg(), mean(), count() methods
    val aggDF1 = df.agg(
      min("Marks").alias("Minimum Marks"),
      max("Marks").alias("Maximum Marks"),
      sum("Marks").alias("Total Marks"),
      avg("Marks").alias("Average Marks"),
      mean("Marks").alias("Mean Marks"),
      count("Marks").alias("Total Count")
    )
    // aggDF.printSchema()
    // aggDF1.show(false)

    // Approach 2 : Finding Min, Max, Sum, Avg, Mean, Count all one by one separately using as(), min(), max(), sum(), avg(), mean(), count() methods
    val minDF1 = df.agg(min("Marks")).alias("Minimum Marks")
    // minDF1.show(false)
    val maxDF1 = df.agg(max("Marks")).alias("Maximum Marks")
    // maxDF1.show(false)
    val sumDF1 = df.agg(sum("Marks")).alias("Total Marks")
    // sumDF1.show(false)
    val avgDF1 = df.agg(avg("Marks")).alias("Average Marks")
    // avgDF1.show(false)
    val meanDF1 = df.agg(mean("Marks")).alias("Mean Marks")
    // meanDF1.show(false)
    val countDF1 = df.agg(count("Marks")).alias("Total Count") //Here count acts as an aggregate function
    // countDF1.show(false)

  }
}
