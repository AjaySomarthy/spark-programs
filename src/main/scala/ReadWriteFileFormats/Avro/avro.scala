package ReadWriteFileFormats.Avro

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// reading an avro file to spark data frame and writing a data frame to avro file
object avro {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing avro files to spark data frame")
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
    df.show(false)

    // first we have to add the below maven dependency in pom.xml if you are using maven project
    // this is the maven dependency for using avro type files in spark data frames

    /*<dependency>
      <groupId>com.databricks</groupId>
      <artifactId>spark-avro_2.11</artifactId>
      <version>4.0.0</version>
    </dependency>*/

    // 1. writing a df to avro file

   /* df.write.format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\employees.avro")*/

    // this throws AnalysisException
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: Failed to find data source: avro.
    // because we used data source in maven pom.xml is com.databricks
    // so we have to use com.databricks.spark.avro here
    /*df.write.format("avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\employees1.avro")*/

    // and df.write.avro("path") doesn't work


    // 2. reading an avro file to a spark data frame

    val avroDF = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\employees.avro")
    // avroDF.printSchema()
    // avroDF.show(false)

    // this throws AnalysisException
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: Failed to find data source: avro.
    // because we used data source in maven pom.xml is com.databricks
    // so we have to use com.databricks.spark.avro here
    /*val avroDF1 = spark.read.format("avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\employees.avro")*/
    // avroDF1.printSchema()
    // avroDF1.show(false)

    // this won't work
    // val avroDF2 = spark.read.avro("")


    // reading multiple avro files into a single df
    val avroDF3 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\employees.avro",
        "C:\\Spark_Sample_Files\\FileFormats\\Avro\\employees_copy.avro")
    // avroDF3.printSchema()
    // avroDF3.show(false)


    // reading all the avro files present in a folder
    val avroDF4 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\Avro_Multiple_Files\\*")
    // avroDF4.printSchema()
    // avroDF4.show(false)


    // avro spark sql
    avroDF.createOrReplaceTempView("Employees")
    val avroDF5 = spark.sql("select * from Employees")
    // avroDF5.printSchema()
    // avroDF5.show(false)


    // saving modes
    // writing to Employees1.avro first time, it will get executed
    /*avroDF.write.format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\Employees1.avro")*/

    // if i do second time, it throws an error says already exists
    /*avroDF.write.format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\Employees1.avro")*/

    // so to overwrite use this
    // this will overwrite the existing file
    /*avroDF.write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\Employees1.avro")*/

    // this will append the data to the existing file
    /*avroDF.write.mode(SaveMode.Append)
      .format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\Employees1.avro")*/

    // this will ignore the operation as Employees1.avro is already existed
    /*avroDF.write.mode(SaveMode.Ignore)
      .format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\Employees1.avro")*/

    // this will throw an error as Employees1.avro is already existed
    /*avroDF.write.mode(SaveMode.ErrorIfExists)
      .format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\Employees1.avro")*/


    val fruitsCols = Seq("Product", "Country", "City", "Rate")
    val fruitsData = Seq(
      ("Apple", "India", "Delhi", 180),
      ("Apple", "India", "Mumbai", 170),
      ("Apple", "USA", "Canada", 200),
      ("Apple", "USA", "Dallas", 190),
      ("Mango", "India", "Delhi", 80),
      ("Mango", "India", "Mumbai", 70),
      ("Mango", "USA", "Canada", 100),
      ("Mango", "USA", "Dallas", 90)
    )
    val fruitsDF = spark.createDataFrame(fruitsData).toDF(fruitsCols: _*)
    // fruitsDF.printSchema()
    // fruitsDF.show(false)


    /*fruitsDF.write.format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits.avro")*/


    // doing partitioning on Product and Country columns
   /* fruitsDF.write.format("com.databricks.spark.avro")
      .partitionBy("Product","Country")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits1.avro")*/

   /* val fruitsAvroDF = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits.avro")
    fruitsAvroDF.printSchema()
    fruitsAvroDF.show(false)

    val fruitsAvroDF1 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits1.avro")
    fruitsAvroDF1.printSchema()
    fruitsAvroDF1.show(false)

    val fruitsAvroDF2 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits1.avro\\Product=Apple")
    fruitsAvroDF2.printSchema()
    fruitsAvroDF2.show(false)

    val fruitsAvroDF3 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits1.avro\\Product=Mango")
    fruitsAvroDF3.printSchema()
    fruitsAvroDF3.show(false)

    val fruitsAvroDF4 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits1.avro\\Product=Apple\\Country=India")
    fruitsAvroDF4.printSchema()
    fruitsAvroDF4.show(false)

    val fruitsAvroDF5 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits1.avro\\Product=Apple\\Country=USA")
    fruitsAvroDF5.printSchema()
    fruitsAvroDF5.show(false)

    val fruitsAvroDF6 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits1.avro\\Product=Mango\\Country=India")
    fruitsAvroDF6.printSchema()
    fruitsAvroDF6.show(false)

    val fruitsAvroDF7 = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\fruits1.avro\\Product=Mango\\Country=USA")
    fruitsAvroDF7.printSchema()
    fruitsAvroDF7.show(false) */




  }

}
