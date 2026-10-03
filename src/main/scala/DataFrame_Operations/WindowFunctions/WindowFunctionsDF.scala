package DataFrame_Operations.WindowFunctions

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionsDF {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("Creating the Window Functions").getOrCreate()

    // Creating a df with sample data
    val cols = Seq("StudentID", "StudentName", "StudentMarks")
    val data = Seq(
      (101, "Anusha", 99),
      (102, "Bindhu", 95),
      (103, "Chitra", 95),
      (104, "Divya", 90)
    )
    val df = spark.createDataFrame(data).toDF(cols: _*)
    // df.printSchema()
    // df.show(false)

    // Finding RowNumber, Rank and DenseRank from the df
    val windowSpec = Window.orderBy(desc("StudentMarks"))
    val windowFunctionsDF = df.withColumn("RowNumber", row_number().over(windowSpec))
      .withColumn("Rank", rank().over(windowSpec))
      .withColumn("DenseRank", dense_rank().over(windowSpec))
    // windowFunctionsDF.printSchema()
    // windowFunctionsDF.show(false)


    // Creating a df1 from CSV file
    val df1 = spark.read.option("header","true")
      .csv("C:\\Spark_Sample_Files\\WindowFunctions\\studentsData.csv")
    // df1.printSchema()
    // df1.show(false)

    // Finding RowNumber, Rank and DenseRank from the df1
    val windowSpec1 = Window.orderBy(desc("StudentMarks"))
    val windowFunctionsDF1 = df1.withColumn("RowNumber1", row_number().over(windowSpec1))
      .withColumn("Rank1", rank().over(windowSpec1))
      .withColumn("DenseRank1", dense_rank().over(windowSpec1))
    // windowFunctionsDF1.printSchema()
    // windowFunctionsDF1.show(false)


    // Creating a df2 from CSV file
    val df2 = spark.read.option("header", "true")
      .csv("src/main/resources/students.csv")
    // df2.printSchema()
    // df2.show(false)

    // Finding RowNumber, Rank and DenseRank from the df2
    val windowSpec2 = Window.orderBy(desc("StudentMarks"))
    val windowFunctionsDF2 = df2.withColumn("RowNumber2", row_number().over(windowSpec2))
      .withColumn("Rank2", rank().over(windowSpec2))
      .withColumn("DenseRank2", dense_rank().over(windowSpec2))
    // windowFunctionsDF2.printSchema()
    // windowFunctionsDF2.show(false)


    // Creating a Temp View from df2 and creating a df3 from it
    df2.createOrReplaceTempView("Students")
    val df3 = spark.sql("select * from Students")
    // df3.printSchema()
    // df3.show(false)

    val windowFunctionsDF3 = spark.sql("select StudentID, StudentName, StudentMarks, " +
      "row_number() over(order by (StudentMarks) desc) as RowNumber3, " +
      "rank() over(order by (StudentMarks) desc) as Rank3, " +
      "dense_rank() over(order by (StudentMarks) desc) as DenseRank3 from Students")
    // windowFunctionsDF3.printSchema()
    // windowFunctionsDF3.show(false)

  }
}
