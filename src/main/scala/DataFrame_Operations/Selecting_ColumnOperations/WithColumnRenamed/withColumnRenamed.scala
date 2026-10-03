package DataFrame_Operations.Selecting_ColumnOperations.WithColumnRenamed

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// operations on a data frame using withColumnRenamed
object withColumnRenamed {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("withColumnRenamed operation on a data frame").getOrCreate()

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

    // renaming the multiple column names of a data frame using withColumnRenamed
    val df1 = df.withColumnRenamed("Name","Person")
      .withColumnRenamed("Branch","Department")
      .withColumnRenamed("Marks","Score")
      .withColumnRenamed("Rank","Order")
    // df1.printSchema()
    // df1.show(false)

  }
}
