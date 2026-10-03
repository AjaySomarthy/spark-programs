package DataFrame_Operations.Selecting_ColumnOperations.WithColumn

import org.apache.log4j._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.SparkSession

// operations on a data frame using withColumn
object withColumn {
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

    // Adding multiple new columns to df
    val df1 = df.withColumn("College",lit("JNTUH"))
      .withColumn("Pincode",lit(123))
    // df1.printSchema()
    // df1.show(false)

    // updating the existing column values of df
    val df2 = df.withColumn("Marks",col("Marks")*10 )
      .withColumn("Rank",col("Rank")*10 )
    // df2.printSchema()
    // df2.show(false)

    // creating multiple new columns from existing columns without changing the data
    val df3 = df.withColumn("Name1", col("Name"))
      .withColumn("Branch1", col("Branch"))
      .withColumn("Marks1",col("Marks"))
      .withColumn("Rank1",col("Rank"))
    // df3.printSchema()
    // df3.show(false)

    // creating multiple new columns from existing columns with changing the data
    val df4 = df.withColumn("Marks1", col("Marks") * 2)
      .withColumn("Marks2", col("Marks") * 3)
      .withColumn("Rank1", col("Rank") * 2)
      .withColumn("Rank2", col("Rank") * 3)
    // df4.printSchema()
    // df4.show(false)

    // changing the column data types (Here integer to string data type)
    val df5 = df.withColumn("Marks",col("Marks").cast("String"))
      .withColumn("Rank",col("Rank").cast("String"))
    // df5.printSchema()
    // df5.show(false)


    df.createTempView("Students1")
    df.createOrReplaceTempView("Students2")

    // Adding multiple new columns to df using spark sql query
    val df6 = spark.sql("select Name, Branch, Marks, Rank, 'JNTUH' as College, 123 as Pincode from Students1")
    // df6.printSchema()
    // df6.show(false)

    // updating the existing column values of df using spark sql query
    val df7 = spark.sql("select Name, Branch, Marks*10 as Marks, Rank*10 as Rank from Students2")
    // df7.printSchema()
    // df7.show(false)

    // creating multiple new columns from existing columns without changing the data
    val df8 = spark.sql("select Name, Branch, Marks, Rank, Name as Name1, Branch as Branch1, " +
      "Marks as Marks1, Rank as Rank1 from Students1")
    // df8.printSchema()
    // df8.show(false)

    // creating multiple new columns from existing columns with changing the data
    val df9 = spark.sql("select Name, Branch, Marks, Rank, Marks*2 as Marks1, Marks*3 as Marks2, " +
      "Rank*2 as Rank1, Rank*3 as Rank2 from Students2")
    // df9.printSchema()
    // df9.show(false)

  }
}
