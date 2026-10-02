package ReadWriteFileFormats

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// Reading and writing CSV, Avro, Parquet, ORC, XML, JSON, Text file formats
object readWriteAllFileFormats {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing all file formats")
      //.enableHiveSupport()
      .getOrCreate()

    val cols = Seq("Name", "Branch", "Marks")
    val data = Seq(
      ("Anusha", "ECE", 85),
      ("Bindhu", "ECE", 95),
      ("Chitra", "ECE", 75),
      ("Divya", "ECE", 98),
      ("Eesha", "CSE", 55),
      ("Fathima", "CSE", 92),
      ("Ganga", "CSE", 35),
      ("Harika", "CSE", 99)
    )
    val df = spark.createDataFrame(data).toDF(cols: _*)
    df.printSchema()
    // df.show(false)


    // 1. CSV file format
    // writing a data frame to a csv file to a location
    /* df.write.option("header","true").option("InferSchema","true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\students.csv") */

    // reading a csv file to Spark data frame
    val studentsCSVDF = spark.read.option("header", "true").option("InferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\students.csv")
    // studentsCSVDF.printSchema()
    // studentsCSVDF.show(false)

    // reading a csv file to Spark data frame
    val personsCSVDF = spark.read.option("header", "true").option("InferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\persons.csv")
    // personsCSVDF.printSchema()
    // personsCSVDF.show(false)


    // 2. Parquet file format
    // writing a data frame to a parquet file
    // df.write.parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\students.parquet")

    // reading a parquet file to spark data frame
    val studentsParquetDF = spark.read
      .parquet("C:\\Spark_Sample_Files\\FileFormats\\Parquet\\students.parquet")
    // studentsParquetDF.printSchema()
    // studentsParquetDF.show(false)


    // 3. Avro file format
    // writing a data frame to an avro file
    /* df.write.format("com.databricks.spark.avro")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Avro\\students.avro") */

    // reading an avro file to spark data frame
    val studentsAvroDF = spark.read.format("com.databricks.spark.avro")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Avro\\students.avro")
    // studentsParquetDF.printSchema()
    // studentsAvroDF.show(false)


    // 4. ORC file format
    // writing a data frame to an orc file
    /* df.write.format("orc")
      .save("C:\\Spark_Sample_Files\\FileFormats\\ORC\\students.orc") */

    // reading an orc file to spark data frame
    val studentsORCDF = spark.read.format("orc")
      .load("C:\\Spark_Sample_Files\\FileFormats\\ORC\\students.orc")
    // studentsORCDF.printSchema()
    // studentsORCDF.show(false)


    // 5. Text file format
    // writing a data frame to a text file
    /* df.write.option("header", "true").option("inferSchema", "true")
      .option("delimiter", ",").csv("C:\\Spark_Sample_Files\\FileFormats\\Text\\students.txt") */

    // reading a text file to spark data frame
    val studentsTextDF = spark.read.option("header", "true").option("inferSchema", "true")
      .option("delimiter", ",").csv("C:\\Spark_Sample_Files\\FileFormats\\Text\\students.txt")
    // studentsTextDF.printSchema()
    // studentsTextDF.show(false)

    val personsTextDF = spark.read.option("header","true").option("inferSchema","true")
      .option("delimiter",",").csv("C:\\Spark_Sample_Files\\FileFormats\\Text\\persons.txt")
    // personsTextDF.printSchema()
    // personsTextDF.show(false)


    // 6. JSON file format
    // writing a data frame to a json file
    // df.write.json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\students.json")

    // reading a json file to spark data frame
    val studentsJSONDF = spark.read.json("C:\\Spark_Sample_Files\\FileFormats\\JSON\\students.json")
    // studentsJSONDF.printSchema()
    // studentsJSONDF.show(false)

    // 7. XML file format
    // writing a data frame to a xml file
    /* df.write.format("com.databricks.spark.xml").option("rootTag","people")
      .option("rowTag","ppl").save("C:\\Spark_Sample_Files\\FileFormats\\XML\\students.xml") */

    // reading a xml file to spark data frame
    val studentsXMLDF = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "ppl").load("C:\\Spark_Sample_Files\\FileFormats\\XML\\students.xml")
    // studentsXMLDF.printSchema()
    // studentsXMLDF.show(false)

  }

}
