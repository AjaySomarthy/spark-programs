package DataFrame_Operations.Selecting_ColumnOperations.Lit

import org.apache.log4j._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.SparkSession

// operations on a data frame using lit() method
object litProg {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("lit operation on a data frame")
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

    // Adding multiple new columns to df using withColumn and lit method
    // lit() method is used to create a new column with data values in it
    val df1 = df.withColumn("College",lit("CBIT"))
      .withColumn("Pincode",lit(456))
    // df1.printSchema()
    // df1.show(false)

  }
}
