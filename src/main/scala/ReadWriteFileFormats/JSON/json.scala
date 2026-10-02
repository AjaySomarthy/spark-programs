package ReadWriteFileFormats.JSON

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// reading a json file to spark data frame and writing a data frame to json file
object json {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing json files to spark data frame")
      .getOrCreate()

    val cols = Seq("Name", "Department", "Salary")
    val data = Seq(
      ("Anusha", "IT", 30500),
      ("Bindhu", "IT", 25000),
      ("Chitra", "IT", 40500),
      ("Divya", "Sales", 70600),
      ("Eesha", "Sales", 22000),
      ("Fathima", "Sales", 10900)
    )
    val df = spark.createDataFrame(data).toDF(cols: _*)
    // df.printSchema()
    // df.show(false)

    // 1. writing a data frame to json file

    // a. using df.write.json("path")
    // df.write.json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees.json")

    // b. using df.write.format("json").save("path")
    /*df.write.format("json")
      .save("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees1.json")*/


    // 2. reading a json file into a data frame

    // a. using df.read.json("path")
    val jsonDF = spark.read.json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees.json")
    // jsonDF.printSchema()
    // jsonDF.show(false)

    // b. using df.read.format("json").save("path")
    val jsonDF1 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees1.json")
    // jsonDF1.printSchema()
    // jsonDF1.show(false)


    // reading manually created json file
    val jsonDF2 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Colleagues1.json")
    // jsonDF2.printSchema()
    // jsonDF2.show(false)

    val jsonDF3 = spark.read.format("json").option("multiline","true")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Colleagues2.json")
    // jsonDF3.printSchema()
    // jsonDF3.show(false)


    // reading multiple json files
    val jsonDF4 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\employees.json",
        "C:\\Spark_Sample_Files\\FileFormats\\JSON\\employees1.json",
        "C:\\Spark_Sample_Files\\FileFormats\\JSON\\Colleagues1.json")
    // jsonDF4.printSchema()
    // jsonDF4.show(false)


    // reading all the files presented in a folder
    val jsonDF5 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\JSON_Multiple_Files\\*")
    // jsonDF5.printSchema()
    // jsonDF5.show(false)


    // json spark sql
    jsonDF.createOrReplaceTempView("Employees")
    val jsonDF6 = spark.sql("select * from Employees")
    // jsonDF6.printSchema()
    // jsonDF6.show(false)


    // saving modes
    // writing to Employees2.json first time, it will get executed
    // jsonDF.write.json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees2.json")

    // if i do second time, it throws an error says already exists
    // jsonDF.write.json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees2.json")

    // so to overwrite use this
    // this will overwrite the existing file
    /*jsonDF.write.mode(SaveMode.Overwrite)
      .json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees2.json")*/

    // this will append the data to the existing file
    /*jsonDF.write.mode(SaveMode.Append)
      .json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees2.json")*/

    // this will ignore the operation as Employees2.json is already existed
    /*jsonDF.write.mode(SaveMode.Ignore)
      .json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees2.json")*/

    // this will throw an error as Employees2.json is already existed
    /*jsonDF.write.mode(SaveMode.ErrorIfExists)
      .json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\Employees2.json")*/


    val fruitsCols = Seq("Product", "Country", "City", "Rate")
    val fruitsData = Seq(
      ("Grapes", "India", "Delhi", 180),
      ("Grapes", "India", "Mumbai", 170),
      ("Grapes", "USA", "Canada", 200),
      ("Grapes", "USA", "Dallas", 190),
      ("Orange", "India", "Delhi", 80),
      ("Orange", "India", "Mumbai", 70),
      ("Orange", "USA", "Canada", 100),
      ("Orange", "USA", "Dallas", 90)
    )
    val fruitsDF = spark.createDataFrame(fruitsData).toDF(fruitsCols: _*)
    // fruitsDF.printSchema()
    // fruitsDF.show(false)


    // partitioning on Product column
    /*fruitsDF.write.partitionBy("Product")
      .json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits.json")*/

    // partitioning on Product and Country columns
    /*fruitsDF.write.partitionBy("Product","Country")
      .json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits1.json")*/


    // reading partitioned data
   /* val fruitsJSONDF = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits.json")
    fruitsJSONDF.printSchema()
    fruitsJSONDF.show(false)

    val fruitsJSONDF1 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits.json\\Product=Grapes")
    fruitsJSONDF1.printSchema()
    fruitsJSONDF1.show(false)

    val fruitsJSONDF2 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits.json\\Product=Orange")
    fruitsJSONDF2.printSchema()
    fruitsJSONDF2.show(false) */


    // reading partitioned data
    /*val fruitsJSONDF3 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits1.json")
    fruitsJSONDF3.printSchema()
    fruitsJSONDF3.show(false)

    val fruitsJSONDF4 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits1.json\\Product=Grapes")
    fruitsJSONDF4.printSchema()
    fruitsJSONDF4.show(false)

    val fruitsJSONDF5 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits1.json\\Product=Orange")
    fruitsJSONDF5.printSchema()
    fruitsJSONDF5.show(false)

    val fruitsJSONDF6 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits1.json\\Product=Grapes\\Country=India")
    fruitsJSONDF6.printSchema()
    fruitsJSONDF6.show(false)

    val fruitsJSONDF7 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits1.json\\Product=Grapes\\Country=USA")
    fruitsJSONDF7.printSchema()
    fruitsJSONDF7.show(false)

    val fruitsJSONDF8 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits1.json\\Product=Orange\\Country=India")
    fruitsJSONDF8.printSchema()
    fruitsJSONDF8.show(false)

    val fruitsJSONDF9 = spark.read.format("json")
      .load("C:\\Spark_Sample_Files\\FileFormats\\JSON\\fruits1.json\\Product=Orange\\Country=USA")
    fruitsJSONDF9.printSchema()
    fruitsJSONDF9.show(false)*/


  }
}