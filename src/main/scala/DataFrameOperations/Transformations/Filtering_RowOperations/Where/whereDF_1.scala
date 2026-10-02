package DataFrameOperations.Transformations.Filtering_RowOperations.Where

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object whereDF_1 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Where usage in data frames")
      .getOrCreate()

    val cols = Seq("Name", "Branch", "Marks")
    val data = Seq(
      ("Anusha", "ECE", 90),
      ("Bindhu", "ECE", 80),
      ("Chitra", "ECE", 70),
      ("Divya", "ECE", 60),
      ("Eesha", "CSE", 90),
      ("Fathima", "CSE", 65),
      ("Ganga", "CSE", 50)
    )

    val df = spark.createDataFrame(data).toDF(cols: _*)
    // df.printSchema()
    // df.show(false)

    // Type 1 :
    val df1 = df.filter("Branch == 'ECE'")
    // df1.printSchema()
    // df1.show(false)

    val df2 = df.where("Marks == '90'")
    // df2.show(false)

    val df3 = df.where("Marks <= '60'")
    // df3.show(false)

    // Type 2 :
    val df4 = df.where(col("Branch") === "CSE")
    // df4.show(false)

    val df5 = df.where(col("Marks") === 90)
    // df5.show(false)

    val df6 = df.where(col("Marks") >= 80)
    // df6.show(false)

    val df7 = df.where(col("Branch") === "CSE" && col("Marks") >= 60)
    // df7.show(false)


    // Type 3 :
    val df8 = df.where(df("Branch") === "CSE")
    // df8.show(false)

    val df9 = df.where(df("Marks") === 90)
    // df9.show(false)

    val df10 = df.where(df("Marks") >= 80)
    // df10.show(false)

    val df11 = df.where(df("Branch") === "CSE" && df("Marks") >= 60)
    // df11.show(false)


    // using Spark SQL
    df.createOrReplaceTempView("Students")
    val df12 = spark.sql("select * from Students where Branch = 'ECE' and Marks >= 80")
    // df12.show(false)

  }
}
