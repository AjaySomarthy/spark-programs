package DataFrameOperations.CreatingDataFrame

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object creatingDataFrame {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark : SparkSession = SparkSession.builder()
      .master("local[1]").appName("Creating Spark DataFrame").getOrCreate()

    import spark.implicits._

    val cols = Seq("Name", "Marks")
    val data = Seq(
      ("Anusha", 85),
      ("Bindhu", 80)
    )

    // 1. creating a data frame from RDD
    val rdd = spark.sparkContext.parallelize(data)

    val dfFromRDD = rdd.toDF()
    // dfFromRDD.printSchema()
    // dfFromRDD.show(false)

    val dfFromRDD1 = rdd.toDF("NAME","MARKS")
    // dfFromRDD1.printSchema()
    // dfFromRDD1.show(false)

    val dfFromRDD2 = rdd.toDF(cols:_*)
    // dfFromRDD2.printSchema()
    // dfFromRDD2.show(false)

    // 2. creating data frame from input data directly

    val df = data.toDF()
    // df.printSchema()
    // df.show(false)

    val df1 = data.toDF("Name1","Marks1")
    // df1.printSchema()
    // df1.show(false)

    val df2 = data.toDF(cols:_*)
    // df2.printSchema()
    // df2.show(false)

    // creating a data frame using createDataFrame() method
    val df3 = spark.createDataFrame(data).toDF()
    // df3.printSchema()
    // df3.show(false)

    val df4 = spark.createDataFrame(data).toDF("NAME","MARKS")
    // df4.printSchema()
    // df4.show(false)

    val df5 = spark.createDataFrame(data).toDF(cols:_*)
    // df5.printSchema()
    // df5.show(false)

    // creating a data frame from reading a CSV file
    val dfFromCSV = spark.read.option("header","true").option("Infer","Schema")
      .csv("C:\\Spark_Sample_Files\\CreatingDataFrame\\employees.csv")
    // dfFromCSV.printSchema()
    // dfFromCSV.show(false)

    // creating a data frame from reading a text file
    val dfFromText = spark.read.option("Header", "True").option("Infer","Schema")
      .text("C:\\Spark_Sample_Files\\CreatingDataFrame\\employees.txt")
    // dfFromText.printSchema()
    // dfFromText.show(false)

    // creating a data frame from reading a json file
    val dfFromJson = spark.read.option("Header", "True").option("Infer", "Schema")
      .json("C:\\Spark_Sample_Files\\CreatingDataFrame\\employees.json")
    // dfFromJson.printSchema()
    // dfFromJson.show(false)

  }
}
