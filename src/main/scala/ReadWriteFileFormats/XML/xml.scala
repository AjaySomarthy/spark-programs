package ReadWriteFileFormats.XML

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// reading an xml file to spark data frame and writing a data frame to xml file
object xml {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing xml files to spark data frame")
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

    // To work on xml files with spark data frames, first add below maven dependency in pom.xml

    /* <!-- Below is maven dependency for spark xml files -->

    <dependency>
        <groupId>com.databricks</groupId>
        <artifactId>spark-xml_2.11</artifactId>
        <version>0.6.0</version>
    </dependency>*/


    // 1. writing a data frame to xml file
    /*df.write.format("com.databricks.spark.xml")
      .option("rootTag","employee")
      .option("rowTag","emp")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees1.xml")*/


    // 2. reading an xml file to spark data frame
    val xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rootTag","employee").option("rowTag","emp")
      .load("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees.xml")
    // xmlDF.printSchema()
    // xmlDF.show(false)


    // reading manually created xml file into a data frame
    val xmlColleaguesDF = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "colleague").option("rowTag", "coll")
      .load("C:\\Spark_Sample_Files\\FileFormats\\XML\\colleagues.xml")
    // xmlColleaguesDF.printSchema()
    // xmlColleaguesDF.show(false)


    // reading multiple xml files to data frame
    val xmlDF1 = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "employee").option("rowTag", "emp")
      .load("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees.xml,C:\\Spark_Sample_Files\\FileFormats\\XML\\employees1.xml")
    // xmlDF1.printSchema()
    // xmlDF1.show(false)


    // reading all the xml files present in a folder
    val xmlDF2 = spark.read.format("com.databricks.spark.xml")
      .option("rootTag","employee").option("rowTag","emp")
      .load("C:\\Spark_Sample_Files\\FileFormats\\XML\\XML_Multiple_Files\\*")
    // xmlDF2.printSchema()
    // xmlDF2.show(false)


    // xml spark sql
    xmlDF.createOrReplaceTempView("Employees")
    val xmlDF3 = spark.sql("select * from Employees")
    // xmlDF3.printSchema()
    // xmlDF3.show(false)


    // saving modes
    // writing to employees2.xml first time, it will get executed
    /*xmlDF.write.format("com.databricks.spark.xml")
      .option("rootTag","employee").option("rowTag","emp")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees2.xml")*/

    // if i do second time, it throws an error says already exists
    /*avroDF.write.format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\Employees1.avro")*/
    /*xmlDF.write.format("com.databricks.spark.xml")
      .option("rootTag", "employee").option("rowTag", "emp")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees2.xml")*/

    // so to overwrite use this
    // this will overwrite the existing file
    /*xmlDF.write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.xml")
      .option("rootTag", "employee").option("rowTag", "emp")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees2.xml")*/

    // this actually should append the data to the existing file
    // But the spark xml datasource does not support the append save mode
    // so it will throw an error
   /* xmlDF.write.mode(SaveMode.Append)
      .format("com.databricks.spark.xml")
      .option("rootTag", "employee").option("rowTag", "emp")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees2.xml")*/

    // this will ignore the operation as employees2.xml is already existed
    /*xmlDF.write.mode(SaveMode.Ignore)
      .format("com.databricks.spark.xml")
      .option("rootTag", "employee").option("rowTag", "emp")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees2.xml")*/

    // this will throw an error as employees2.xml is already existed
    /*xmlDF.write.mode(SaveMode.ErrorIfExists)
      .format("com.databricks.spark.xml")
      .option("rootTag", "employee").option("rowTag", "emp")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\employees2.xml")*/


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

    /*fruitsDF.write.format("com.databricks.spark.xml")
      .option("rootTag","fruit").option("rowTag","fru")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\fruits.xml")*/

   /* fruitsDF.write.format("com.databricks.spark.xml")
      .option("rootTag", "fruit").option("rowTag", "fru")
      .partitionBy("Product")
      .save("C:\\Spark_Sample_Files\\FileFormats\\XML\\fruits1.xml")*/

    val fruitsXMLDF = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "fruit").option("rowTag", "fru")
      .load("C:\\Spark_Sample_Files\\FileFormats\\XML\\fruits.xml")
    // fruitsXMLDF.printSchema()
    // fruitsXMLDF.show(false)

    val fruitsXMLDF1 = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "fruit").option("rowTag", "fru")
      .load("C:\\Spark_Sample_Files\\FileFormats\\XML\\fruits1.xml")
    // fruitsXMLDF1.printSchema()
    // fruitsXMLDF1.show(false)

    // getting an error and need to fix this
    /*val fruitsXMLDF2 = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "fruit").option("rowTag", "fru")
      .load("C:\\Spark_Sample_Files\\FileFormats\\XML\\fruits1.xml\\Product=Grapes")
    fruitsXMLDF2.printSchema()
    fruitsXMLDF2.show(false)*/

    // getting an error and need to fix this
   /* val fruitsXMLDF3 = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "fruit").option("rowTag", "fru")
      .load("C:\\Spark_Sample_Files\\FileFormats\\XML\\fruits1.xml\\Product=Orange")
    fruitsXMLDF3.printSchema()
    fruitsXMLDF3.show(false)*/

}
}
