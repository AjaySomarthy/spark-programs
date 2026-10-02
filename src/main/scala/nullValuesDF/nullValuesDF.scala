package nullValuesDF

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

// Creating a DF with null values
object nullValuesDF {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Creating a DF with null values")
      .getOrCreate()

    // Creating a data frame with null values in it
    // Option 1 : Using java.lang.Integer (Most Common)
    val cols = Seq("Name","Marks")
    val data : Seq[(String, java.lang.Integer)] = Seq(
      ("Anusha", 96),
      (null, 85),
      ("Chitra", null),
      (null, null)
    )
    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.printSchema()
    // df.show(false)


    // Option 2 : Using Scala's Option Type
    val cols1 = Seq("Name","Salary")
    val data1 = Seq(
      (Some("Anusha"), Some(20500)),
      (None, Some(35000)),
      (Some("Chitra"), None),
      (None, None)
    )
    val df1 = spark.createDataFrame(data1).toDF(cols1: _*)
    // df1.printSchema()
    // df1.show(false)


    // Option 3 : Using a StructType Schema
    val data2 = Seq(
      Row("Anusha", 85),
      Row(null, 80),
      Row("Chitra", null),
      Row(null, null)
    )
    val schema = StructType(Seq(
      StructField("Name", StringType, nullable = true),
      StructField("Marks", IntegerType, nullable = true)
    ))
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2),schema)
    // df2.printSchema()
    // df2.show(false)

  }
}
