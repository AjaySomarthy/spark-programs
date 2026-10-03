package DataFrame_Operations.Selecting_ColumnOperations.Select

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// selecting a single column or multiple columns from a data frame programme
object select {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("select operation on a data frame")
      .getOrCreate()

    val cols = Seq("Name", "Branch", "Marks", "Rank")
    val data = Seq(
      ("Anusha", "ECE", 90, 1),
      ("Bindhu", "CSE", 80, 2),
      ("Chitra", "Civil", 70, 3),
      ("Divya", "Mech", 60, 4)
    )

    val df = spark.createDataFrame(data).toDF(cols: _*)
    // df.printSchema()
    // df.show(false)

    // selecting a single column from a data frame
    val df1 = df.select("Branch")
    // df1.printSchema()
    // df1.show(false)

    // selecting multiple columns from a data frame
    val df2 = df.select("Name","Marks")
    // df2.printSchema()
    // df2.show(false)

    // selecting multiple columns from a data frame
    val df3 = df.select("Name", "Marks", "Rank")
    // df3.printSchema()
    // df3.show(false)

  }
}
