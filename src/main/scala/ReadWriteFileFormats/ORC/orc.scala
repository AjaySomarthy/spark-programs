package ReadWriteFileFormats.ORC

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// reading an orc file to spark data frame and writing a data frame to orc file
object orc {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing orc files to spark data frame")
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

    // To work on orc files with spark data frames, first add below maven dependency in pom.xml

    /* <!-- Below is maven dependency for spark orc files -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-hive_2.11</artifactId>
        <version>2.3.1</version>
    </dependency> */

    // 1. writing a data frame to an orc file

    // a. using df.write.orc("path")
    // its working without enableHiveSupport
    // df.write.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\employees.orc")

    // b. using df.format("orc").save("path")
    // df.write.format("orc").save("C:\\Spark_Sample_Files\\FileFormats\\ORC\\employees1.orc")


    // 2. reading an orc file to a spark data frame

    // a. using spark.read.orc("path")
    val orcDF = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\employees.orc")
    // orcDF.printSchema()
    // orcDF.show(false)

    // b. using spark.read.format("orc").load("path")
    val orcDF1 = spark.read.format("orc")
      .load("C:\\Spark_Sample_Files\\FileFormats\\ORC\\employees1.orc")
    // orcDF1.printSchema()
    // orcDF1.show(false)


    // reading multiple orc files to spark data frame
    val orcDF2 = spark.read.format("orc")
      .load("C:\\Spark_Sample_Files\\FileFormats\\ORC\\employees.orc",
        "C:\\Spark_Sample_Files\\FileFormats\\ORC\\employees1.orc")
    // orcDF2.printSchema()
    // orcDF2.show(false)


    // reading all the orc files present in a directory to spark data frame
    val orcDF3 = spark.read.format("orc")
      .load("C:\\Spark_Sample_Files\\FileFormats\\ORC\\ORC_Multiple_Files\\*")
    // orcDF3.printSchema()
    // orcDF3.show(false)


    // orc spark sql
    orcDF1.createOrReplaceTempView("Employees")
    // val orcDF4 = spark.sql("select * from Employees")
    // orcDF4.printSchema()
    // orcDF4.show(false)


    // saving modes
    // writing to Employees2.orc first time, it will get executed
    // orcDF.write.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\Employees2.orc")

    // if i do second time, it throws an error says already exists
    // orcDF.write.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\Employees2.orc")

    // so to overwrite use this
    // this will overwrite the existing file
    /*orcDF.write.mode(SaveMode.Overwrite)
      .orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\Employees2.orc")*/

    // this will append the data to the existing file
    /*orcDF.write.mode(SaveMode.Append)
      .orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\Employees2.orc")*/

    // this will ignore the operation as Employees2.orc is already existed
    /*orcDF.write.mode(SaveMode.Ignore)
      .orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\Employees2.orc")*/

    // this will throw an error as Employees2.orc is already existed
    /*orcDF.write.mode(SaveMode.ErrorIfExists)
      .orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\Employees2.orc")*/


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

    // fruitsDF.write.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits.orc")


    // doing partitioning on Product and Country columns
    /* fruitsDF.write.format("orc").partitionBy("Product","Country")
         .save("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits1.orc")*/

   /* val fruitsORCDF = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits.orc")
    fruitsORCDF.printSchema()
    fruitsORCDF.show(false)

    val fruitsORCDF1 = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits1.orc")
    fruitsORCDF1.printSchema()
    fruitsORCDF1.show(false)

    val fruitsORCDF2 = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits1.orc\\Product=Grapes")
    fruitsORCDF2.printSchema()
    fruitsORCDF2.show(false)

    val fruitsORCDF3 = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits1.orc\\Product=Orange")
    fruitsORCDF3.printSchema()
    fruitsORCDF3.show(false)

    val fruitsORCDF4 = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits1.orc\\Product=Grapes\\Country=India")
    fruitsORCDF4.printSchema()
    fruitsORCDF4.show(false)

    val fruitsORCDF5 = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits1.orc\\Product=Grapes\\Country=USA")
    fruitsORCDF5.printSchema()
    fruitsORCDF5.show(false)

    val fruitsORCDF6 = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits1.orc\\Product=Orange\\Country=India")
    fruitsORCDF6.printSchema()
    fruitsORCDF6.show(false)

    val fruitsORCDF7 = spark.read.orc("C:\\Spark_Sample_Files\\FileFormats\\ORC\\fruits1.orc\\Product=Orange\\Country=USA")
    fruitsORCDF7.printSchema()
    fruitsORCDF7.show(false) */

  }
}
