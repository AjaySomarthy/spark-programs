package Interviews_Spark.SecondHighestMarks

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object secondHighestMarksWithDepartmentWiseDF {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Creating the Window Functions").getOrCreate()

    // Creating a df with sample data
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
    // df.printSchema()
    // df.show(false)

    // Finding the DenseRank
    val windowSpec = Window.partitionBy("DepartmentName").orderBy(desc("StudentMarks"))
    val denseRankDF = df.withColumn("DenseRank", dense_rank().over(windowSpec))
    // denseRankDF.printSchema()
    // denseRankDF.show(false)

    // Finding second highest marks from df for each department
    val secondHighestDF = denseRankDF.select("StudentID", "StudentName", "StudentMarks", "DepartmentName")
      .where(col("DenseRank") === 2)
    // secondHighestDF.printSchema()
    // secondHighestDF.show(false)

    // Finding third highest marks from df for each department
    val thirdHighestDF = denseRankDF.select("StudentID", "StudentName", "StudentMarks", "DepartmentName")
      .where(col("DenseRank") === 3)
    // thirdHighestDF.printSchema()
    // thirdHighestDF.show(false)


    // Creating a df1 from CSV file
    val df1 = spark.read.option("header", "true")
      .csv("C:\\Spark_Sample_Files\\SecondHighestMarks\\studentsDataDepartmentWise.csv")
    // df1.printSchema()
    // df1.show(false)

    // Finding the DenseRank
    val windowSpec1 = Window.partitionBy("DepartmentName").orderBy(desc("StudentMarks"))
    val denseRankDF1 = df1.withColumn("DenseRank", dense_rank().over(windowSpec1))
    // denseRankDF1.printSchema()
    // denseRankDF1.show(false)

    // Finding second highest marks from each department wise
    val secondHighestDF1 = denseRankDF1.select("StudentID", "StudentName", "StudentMarks")
      .where(col("DenseRank") === 2)
    // secondHighestDF1.printSchema()
    // secondHighestDF1.show(false)

    // Finding third highest marks from each department wise
    val thirdHighestDF1 = denseRankDF1.select("StudentID", "StudentName", "StudentMarks")
      .where(col("DenseRank") === 3)
    // thirdHighestDF1.printSchema()
    // thirdHighestDF1.show(false)


    // Creating a df2 from CSV file
    val df2 = spark.read.option("header", "true")
      .csv("src/main/resources/studentsDepartment.csv")
    // df2.printSchema()
    // df2.show(false)

    // Finding the DenseRank
    val windowSpec2 = Window.partitionBy("DepartmentName").orderBy(desc("StudentMarks"))
    val denseRankDF2 = df2.withColumn("DenseRank", dense_rank().over(windowSpec2))
    // denseRankDF2.printSchema()
    // denseRankDF2.show(false)

    // Finding second highest marks from each department wise
    val secondHighestDF2 = denseRankDF2.select("StudentID", "StudentName", "StudentMarks")
      .where(col("DenseRank") === 2)
    // secondHighestDF2.printSchema()
    // secondHighestDF2.show(false)

    // Finding third highest marks from each department wise
    val thirdHighestDF2 = denseRankDF2.select("StudentID", "StudentName", "StudentMarks")
      .where(col("DenseRank") === 3)
    // thirdHighestDF2.printSchema()
    // thirdHighestDF2.show(false)


    // Creating a Temp View from df2 and creating a df3 from it
    df2.createOrReplaceTempView("Students")
    val df3 = spark.sql("select * from Students")
    // df3.printSchema()
    // df3.show(false)

    val secondHighestDF3 = spark.sql("select StudentID, StudentName, StudentMarks " +
      "from (select StudentID, StudentName, StudentMarks, " +
      "dense_rank() over(partition by (DepartmentName) order by (StudentMarks) desc) as DenseRank from Students) " +
      "where DenseRank=2")
    // secondHighestDF3.printSchema()
    // secondHighestDF3.show(false)

    val thirdHighestDF3 = spark.sql("select StudentID, StudentName, StudentMarks " +
      "from (select StudentID, StudentName, StudentMarks, " +
      "dense_rank() over(partition by (DepartmentName) order by (StudentMarks) desc) as DenseRank from Students) " +
      "where DenseRank=3")
    // thirdHighestDF3.printSchema()
    // thirdHighestDF3.show(false)

  }
}
