package DataFrameOperations.Selecting_ColumnOperations.Col

import org.apache.log4j._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.SparkSession

// operations on a data frame using col() method
object colProg {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("withColumn operation on a data frame")
      .getOrCreate()

    // creating a data frame with sample data
    val cols = Seq("Name", "Branch", "Marks", "Rank")
    val data = Seq(
      ("Anusha", "ECE", 90, 1),
      ("Bindhu", "CSE", 80, 2),
      ("Chitra", "IT", 70, 3),
      ("Divya", "Civil", 60, 4)
    )

    val df = spark.createDataFrame(data).toDF(cols: _*)
    // df.printSchema()
    // df.show(false)

    // updating the existing column values of df using withColumn and col() method
    val df1 = df.withColumn("Marks",col("Marks")*10 )
      .withColumn("Rank",col("Rank")*10 )
    // df1.printSchema()
    // df1.show(false)

    // creating multiple new columns from existing columns without changing the data using withColumn and col() method
    val df2 = df.withColumn("Name1", col("Name"))
      .withColumn("Branch1", col("Branch"))
      .withColumn("Marks1",col("Marks"))
      .withColumn("Rank1",col("Rank"))
    // df2.printSchema()
    // df2.show(false)

    // creating multiple new columns from existing columns with changing the data using withColumn and col() method
    val df3 = df.withColumn("Marks1", col("Marks") * 2)
      .withColumn("Marks2", col("Marks") * 3)
      .withColumn("Rank1", col("Rank") * 2)
      .withColumn("Rank2", col("Rank") * 3)
    // df3.printSchema()
    // df3.show(false)

    // changing the column data types using withColumn and col() method (Here integer to string data type)
    val df4 = df.withColumn("Marks",col("Marks").cast("String"))
      .withColumn("Rank",col("Rank").cast("String"))
    // dfval studentsCols = Seq("Name", "Branch", "Marks")
    //    val studentsData = Seq(
    //      ("Anusha", "ECE", 90),
    //      ("Bindhu", "ECE", 80),
    //      ("Chitra", "ECE", 70),
    //      ("Divya", "ECE", 60),
    //      ("Eesha", "CSE", 90),
    //      ("Fathima", "CSE", 65),
    //      ("Ganga", "CSE", 50)
    //    )
    //
    //    val studentsDF = spark.createDataFrame(studentsData).toDF(studentsCols: _*)
    //    // studentsDF.printSchema()
    //    // studentsDF.show(false)4.printSchema()
    // df4.show(false)





    val studentsDF1 = df.where(col("Branch") === "CSE")
    // studentsDF1.show(false)

    val studentsDF2 = df.where(col("Marks") === 90)
    // studentsDF2.show(false)

    val studentsDF3 = df.where(col("Marks") >= 80)
    // studentsDF3.show(false)

    val studentsDF4 = df.where(col("Branch") === "CSE" && col("Marks") >= 60)
    // studentsDF4.show(false)

    // we use col() method in many places

  }
}
