package ReadWriteFileFormats.Text

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// reading a text file to spark data frame and writing a data frame to a text file
object text {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing text files to spark data frame")
      .getOrCreate()

    import spark.implicits._
    val cols = Seq("Name")
    val data = Seq(
      ("Anusha"),
      ("Bindhu"),
      ("Chitra")
    )
    val df = data.toDF(cols: _*)
    // df.printSchema()
    // df.show(false)

    // we can convert a text file to df only with single column

    // 1. writing a text file into spark data frame

    // a. using df.write.text("path")
    // it will write only one column df to text file
    /*df.write
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees.txt")

    df.write.option("header","true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees1.txt")

    df.write.option("header", "true").option("inferSchema","true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees2.txt")*/


    // b. using df.write.format("text").save("path")
    // it will write only one column df to text file
    /*df.write.format("text")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees3.txt")*/

    /*df.write.format("text").option("header","true")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees4.txt")*/

   /* df.write.format("text").option("header", "true").option("inferSchema","true")
      .save("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees5.txt")*/


    // 2. reading a text file into a df

    // a. using spark.read.text("path")
    // it will read multi column text file into a df
    // but all column values comes under one column value with string type
    val textDF = spark.read.text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees2.txt")
    // textDF.printSchema()
    // textDF.show(false)

    val textDF1 = spark.read.option("header","true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees2.txt")
    // textDF1.printSchema()
    // textDF1.show(false)

    val textDF2 = spark.read.option("header","true").option("inferSchema","true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees2.txt")
    // textDF2.printSchema()
    // textDF2.show(false)

    // b. using spark.read.format("text").load("path")
    // it will read multi column text file into a df
    // but all column values comes under one column value with string type
    val textDF3 = spark.read.format("text")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees5.txt")
    // textDF3.printSchema()
    // textDF3.show(false)

    val textDF4 = spark.read.format("text")
      .option("header", "true")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees5.txt")
    // textDF4.printSchema()
    // textDF4.show(false)

    val textDF5 = spark.read.format("text")
      .option("header", "true").option("inferSchema", "true")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees5.txt")
    // textDF5.printSchema()
    // textDF5.show(false)

    // we can use any method, all will give same result
    // header name is value always

    // reading manually created file
    /*val textDF6 = spark.read.format("text")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Text\\persons.txt")
    textDF6.printSchema()
    textDF6.show(false)

    val textDF7 = spark.read.format("text")
      .option("header", "true").option("inferSchema", "true")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Text\\persons.txt")
    textDF7.printSchema()
    textDF7.show(false)*/

    // reading multiple text files into a single df
    val textDF7 = spark.read.format("text")
      .option("header", "true").option("inferSchema", "true")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees3.txt",
        "C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees4.txt")
    // textDF7.printSchema()
    // textDF7.show(false)


    // reading all the text files present in a directory into a single df
    val textDF8 = spark.read.format("text")
      .option("header", "true").option("inferSchema", "true")
      .load("C:\\Spark_Sample_Files\\FileFormats\\Text\\Text_Multiple_Files\\*")
    // textDF8.printSchema()
    // textDF8.show(false)

    // text spark sql
    textDF5.createOrReplaceTempView("Employees")
    val textDF9 = spark.sql("select * from Employees")
    // textDF9.printSchema()
    // textDF9.show(false)


    // saving modes

    // creating employees6.text
    /*textDF9.write.option("header","true").option("inferSchema","true")
       .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees6.txt")*/

    // if i do the same as above, it will throw an error and says file already existed
    /*textDF9.write.option("header", "true").option("inferSchema", "true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees6.txt")*/

    // It will overwrite the existing file
    /*textDF9.write.mode(SaveMode.Overwrite)
      .option("header", "true").option("inferSchema", "true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees6.txt")*/

    // this will append the data to the existing file
    /*textDF9.write.mode(SaveMode.Append)
      .option("header", "true").option("inferSchema", "true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees6.txt")*/

    // this will ignore write operation as employees6.txt is already existed
    /*textDF9.write.mode(SaveMode.Ignore)
      .option("header", "true").option("inferSchema", "true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees6.txt")*/

    // It will throw an error if the file is already existed
     /*textDF9.write.mode(SaveMode.ErrorIfExists)
      .option("header", "true").option("inferSchema", "true")
      .text("C:\\Spark_Sample_Files\\FileFormats\\Text\\Employees6.txt")*/

  }
}