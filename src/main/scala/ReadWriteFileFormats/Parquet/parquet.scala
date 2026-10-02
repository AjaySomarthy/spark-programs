package ReadWriteFileFormats.Parquet

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// reading a parquet file to spark data frame and writing a data frame to parquet file
object parquet {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing parquet files to spark data frame")
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

    // 1. writing df to parquet file

    // a. using df.write.parquet("path")
    // this will give proper output
    // df.write.parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees.parquet")

    // so no need to mention .option("header","true") for parquet files
    /* df.write.option("header","true")
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees1.parquet") */

    // and no need to mention .option("header","true") and .option("inferSchema","true") for parquet files
    /* df.write.option("header", "true").option("inferSchema","true")
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees2.parquet") */

    // b. using df.write.format("parquet").save("path")
    // no need to use options, as this will give proper output
    // df.write.format("parquet").save("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees3.parquet")


    // 2. reading a parquet file to a data frame

    // a. using spark.read.parquet("path")
    // this will give proper output
    val parquetDF = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees2.parquet")
    // parquetDF.printSchema()
    // parquetDF.show(false)

    // so no need to mention .option("header","true") for parquet files
    val parquetDF1 = spark.read.option("header","true")
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees2.parquet")
    // parquetDF1.printSchema()
    // parquetDF1.show(false)

    // and no need to mention .option("header","true") and .option("inferSchema","true") for parquet files
    val parquetDF2 = spark.read.option("header","true").option("inferSchema","true")
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees2.parquet")
    // parquetDF2.printSchema()
    // parquetDF2.show(false)

    // b. using spark.read.format("parquet").load("path")
    // no need to use options, as this will give proper output
    val parquetDF3 = spark.read.format("parquet")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees3.parquet")
    // parquetDF3.printSchema()
    // parquetDF3.show(false)


    // reading multiple parquet files into a single data frame
    val parquetDF4 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees.parquet",
        "C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees1.parquet",
        "C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees2.parquet")
    // parquetDF4.printSchema()
    // parquetDF4.show(false)


    // reading all the parquet files from a directory
    val parquetDF5 = spark.read.format("parquet")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\Parquet_Multiple_Files\\*")
    // parquetDF5.printSchema()
    // parquetDF5.show(false)


    // parquet spark sql
    parquetDF3.createOrReplaceTempView("Employees")
    val parquetDF6 = spark.sql("select * from Employees")
    // parquetDF6.printSchema()
    // parquetDF6.show(false)


    // saving modes
    // writing to Employees5.parquet first time, it will get executed
    // parquetDF.write.parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\Employees5.parquet")

    // if i do second time, it throws an error says already exists
    // parquetDF.write.parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\Employees5.parquet")

    // so to overwrite use this
    // this will overwrite the existing file
    /*parquetDF.write.mode(SaveMode.Overwrite)
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\Employees5.parquet")*/

    // this will append the data to the existing file
    /*parquetDF.write.mode(SaveMode.Append)
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\Employees5.parquet")*/

    // this will ignore the operation as Employees5.parquet is already existed
    /*parquetDF.write.mode(SaveMode.Ignore)
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\Employees5.parquet")*/

    // this will throw an error as Employees5.parquet is already existed
    /*parquetDF.write.mode(SaveMode.ErrorIfExists)
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\Employees5.parquet")*/


    // doing partitioning on Department column
    /* parquetDF3.write.partitionBy("Department")
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees4.parquet") */

    val parquetDF7 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees4.parquet")
    // parquetDF7.printSchema()
    // parquetDF7.show(false)

    val parquetDF8 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees4.parquet\\Department=IT")
    // parquetDF8.printSchema()
    // parquetDF8.show(false)

    val parquetDF9 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\employees4.parquet\\Department=Sales")
    // parquetDF9.printSchema()
    // parquetDF9.show(false)


    val fruitsCols = Seq("Product","Country","City","Rate")
    val fruitsData = Seq(
      ("Apple","India","Delhi",180),
      ("Apple","India","Mumbai",170),
      ("Apple","USA","Canada",200),
      ("Apple","USA","Dallas",190),
      ("Mango","India","Delhi",80),
      ("Mango","India","Mumbai",70),
      ("Mango","USA","Canada",100),
      ("Mango","USA","Dallas",90)
    )
    val fruitsDF = spark.createDataFrame(fruitsData).toDF(fruitsCols: _*)
    // fruitsDF.printSchema()
    // fruitsDF.show(false)

    // writing the fruits df to a parquet file without any partitions on it
    /*fruitsDF.write
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits.parquet")*/


    // writing the fruits df to a parquet file with 2 partitions on it
    // doing partitions on both Product and Country columns
   /* fruitsDF.write.partitionBy("Product","Country")
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits1.parquet")*/

    val fruitsParquetDF = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits.parquet")
    // fruitsParquetDF.printSchema()
    // fruitsParquetDF.show(false)

    val fruitsParquetDF1 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits1.parquet")
    // fruitsParquetDF1.printSchema()
    // fruitsParquetDF1.show(false)

    val fruitsParquetDF2 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits1.parquet\\Product=Apple")
    // fruitsParquetDF2.printSchema()
    // fruitsParquetDF2.show(false)

    val fruitsParquetDF3 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits1.parquet\\Product=Mango")
    // fruitsParquetDF3.printSchema()
    // fruitsParquetDF3.show(false)

    val fruitsParquetDF4 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits1.parquet\\Product=Apple\\Country=India")
    // fruitsParquetDF4.printSchema()
    // fruitsParquetDF4.show(false)

    val fruitsParquetDF5 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits1.parquet\\Product=Apple\\Country=USA")
    // fruitsParquetDF5.printSchema()
    // fruitsParquetDF5.show(false)

    val fruitsParquetDF6 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits1.parquet\\Product=Mango\\Country=India")
    // fruitsParquetDF6.printSchema()
    // fruitsParquetDF6.show(false)

    val fruitsParquetDF7 = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\fruits1.parquet\\Product=Mango\\Country=USA")
    // fruitsParquetDF7.printSchema()
    // fruitsParquetDF7.show(false)



  }
}
