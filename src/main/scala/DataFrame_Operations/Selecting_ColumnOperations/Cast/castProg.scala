package DataFrame_Operations.Selecting_ColumnOperations.Cast

import org.apache.log4j._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.SparkSession

// operations on a data frame using cast() method
object castProg {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("cast operation on a data frame")
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

    // changing the column data types using withColumn, col() and cast() methods (Here integer to string data type)
    // cast() method is used to change the data types of a column
    val df5 = df.withColumn("Marks",col("Marks").cast("String"))
      .withColumn("Rank",col("Rank").cast("String"))
    // df5.printSchema()
    // df5.show(false)

  }
}
