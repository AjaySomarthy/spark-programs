package ReadWriteFileFormats.CSV

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// reading a csv file to spark data frame and writing a data frame to csv file
object csv {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing csv files to spark data frame")
      .getOrCreate()

    val cols = Seq("Name","Department","Salary")
    val data = Seq(
      ("Anusha","IT",30500),
      ("Bindhu","IT",25000),
      ("Chitra","IT",40500),
      ("Divya","Sales",70600),
      ("Eesha","Sales",22000),
      ("Fathima","Sales",10900)
    )
    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.printSchema()
    // df.show(false)

    // 1. writing a data frame to a csv file

    // a. using df.write.csv("path")

    // its not recommended
    // it only store the column values not the column names, so data loss happens with this
    // df.write.csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees.csv")

    // it stores both column values and column names, so there is no data loss
    // option("header","true") is same as option("header",true)
    // df.write.option("header","true").csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees1.csv")

    // this is the best approach
    // it also stores both column values and column names, so there is no data loss
    /*  df.write.option("header","true").option("inferSchema","true")
       .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees2.csv") */

    // b. using df.write.format("csv").save("path")

    // its not recommended
    // it only store the column values not the column names, so data loss happens with this
    // df.write.format("csv").save("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees3.csv")

    // it stores both column values and column names, so there is no data loss
    // option("header","true") is same as option("header",true)
    /* df.write.format("csv").option("header","true")
      .save("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees4.csv") */

    // this is the best approach
    // it also stores both column values and column names, so there is no data loss
    /* df.write.format("csv").option("header","true").option("inferSchema","true")
      .save("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees5.csv") */


    // 2. reading a csv file into spark data frame

    // a. using spark.read.csv("path")

    // its not recommended
    // it read the column names as column values, integer types as string type
    val csvDF = spark.read.csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees2.csv")
    // csvDF.printSchema()
    // csvDF.show(false)

    // its not recommended
    // Column names are set to be column names only and it reads integer types as string type
    val csvDF1 = spark.read.option("header","true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees2.csv")
    // csvDF1.printSchema()
    // csvDF1.show(false)

    // this is the best approach
    // it reads string as string type and integer as integer type
    val csvDF2 = spark.read.option("header", "true").option("inferSchema","true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees2.csv")
    // csvDF2.printSchema()
    // csvDF2.show(false)


    // b. using spark.read.format("csv").load("path")

    // its not recommended
    // it read the column names as column values, integer types as string type
    val csvDF3 = spark.read.format("csv")
      .load("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees5.csv")
    // csvDF3.printSchema()
    // csvDF3.show(false)

    // its not recommended
    // Column names are set to be column names only and it reads integer types as string type
    val csvDF4 = spark.read.format("csv").option("header","true")
      .load("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees5.csv")
    // csvDF4.printSchema()
    // csvDF4.show(false)

    // this is the best approach
    val csvDF5 = spark.read.format("csv").option("header", "true").option("inferSchema","true")
      .load("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees5.csv")
    // csvDF5.printSchema()
    // csvDF5.show(false)

    // for better approach use .option("header","true") and .option("inferSchema","true")

    // reading manually created file
    val colleaguesDF = spark.read.option("header","true").option("inferSchema","true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\colleagues.csv")
    // colleaguesDF.printSchema()
    // colleaguesDF.show(false)


    // using multiple options at a time to read a csv file into a df
    val allOptions = Map("header"->"true","inferSchema"->"true","delimiter"->",")
    val df1 = spark.read.options(allOptions).csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees5.csv")
    // df1.printSchema()
    // df1.show(false)


    // reading multiple csv files
    val df2 = spark.read.option("header","true").option("inferSchema","true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees3.csv",
        "C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees4.csv",
        "C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees5.csv")
    // df2.printSchema()
    // df2.show(false)


    // reading all the csv files from a directory
     val df3 = spark.read.option("header", "true").option("inferSchema", "true")
       .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\CSV_Multiple_Files\\*")
    // df3.printSchema()
    // df3.show(false)

    // cache dataframe
    // val df4 = df3.cache()
    // df4.show(false)


    // csv spark sql
    df1.createOrReplaceTempView("Employees")
    // spark.sql("select * from Employees").show(false)


    // creating employees6.csv
   /* df1.write.option("header","true").option("inferSchema","true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees6.csv")*/

    // if i do the same as above, it will throw an error and says file already existed
    /* df1.write.option("header","true").option("inferSchema","true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees6.csv") */


    // saving modes
    // It will overwrite the existing file
    /* df1.write.mode(SaveMode.Overwrite)
      .option("header", "true").option("inferSchema","true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees6.csv") */

    // this will append the data to the existing file
    /* df1.write.mode(SaveMode.Append)
      .option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees6.csv") */

    // this will ignore write operation as employees6.csv is already existed
    /* df1.write.mode(SaveMode.Ignore)
      .option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees6.csv") */

    // It will throw an error if the file is already existed
    /* df1.write.mode(SaveMode.ErrorIfExists)
      .option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees6.csv") */


    // doing partitions department wise
    /* csvDF5.write.format("csv").option("header", "true").option("inferSchema", "true")
       .partitionBy("Department")
       .save("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees7.csv") */

    val partitionCSVDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees7.csv")
    // partitionCSVDF.printSchema()
    // partitionCSVDF.show(false)

    val partitionCSVDF1 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees7.csv\\Department=IT")
    // partitionCSVDF1.printSchema()
    // partitionCSVDF1.show(false)

    val partitionCSVDF2 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\employees7.csv\\Department=Sales")
    // partitionCSVDF2.printSchema()
    // partitionCSVDF2.show(false)


    val partitionCSVDF3 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits.csv")
    // partitionCSVDF3.printSchema()
    // partitionCSVDF3.show(false)


    // doing partition by Product column
   /* partitionCSVDF3.write.option("header", "true").option("inferSchema", "true")
      .partitionBy("Product")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits1.csv")*/

    val partitionCSVDF4 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits1.csv")
    // partitionCSVDF4.printSchema()
    // partitionCSVDF4.show(false)

    val partitionCSVDF5 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits1.csv\\Product=Apple")
    // partitionCSVDF5.printSchema()
    // partitionCSVDF5.show(false)

    val partitionCSVDF6 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits1.csv\\Product=Banana")
    // partitionCSVDF6.printSchema()
    // partitionCSVDF6.show(false)


    // doing partition by 2 columns Product and Country
    /*partitionCSVDF3.write.option("header", "true").option("inferSchema", "true")
      .partitionBy("Product","Country")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits2.csv")*/

    val partitionCSVDF7 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits2.csv")
    // partitionCSVDF7.printSchema()
    // partitionCSVDF7.show(false)

    val partitionCSVDF8 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits2.csv\\Product=Apple")
    // partitionCSVDF8.printSchema()
    // partitionCSVDF8.show(false)

    val partitionCSVDF9 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits2.csv\\Product=Banana")
    // partitionCSVDF9.printSchema()
    // partitionCSVDF9.show(false)

    val partitionCSVDF10 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits2.csv\\Product=Apple\\Country=India")
    // partitionCSVDF10.printSchema()
    // partitionCSVDF10.show(false)

    val partitionCSVDF11 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits2.csv\\Product=Apple\\Country=USA")
    // partitionCSVDF11.printSchema()
    // partitionCSVDF11.show(false)

    val partitionCSVDF12 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits2.csv\\Product=Banana\\Country=India")
    // partitionCSVDF12.printSchema()
    // partitionCSVDF12.show(false)

    val partitionCSVDF13 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\FileFormats\\CSV\\fruits2.csv\\Product=Banana\\Country=USA")
    // partitionCSVDF13.printSchema()
    // partitionCSVDF13.show(false)

  }
}
