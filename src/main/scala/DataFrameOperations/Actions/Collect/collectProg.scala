package DataFrameOperations.Actions.Collect

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object collectProg {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Collect action usage").getOrCreate()

    val cols = Seq("Name","Marks")
    val data = Seq(
      ("Anusha",90),
      ("Bindhu",85),
      ("Chitra",80),
      ("Divya",70)
    )
    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.printSchema()
    // df.show(false)

    // Usage 1 :
    // Here It will return the first n rows of a data frame
    // collect() action returns an Array[Row] containing the first n rows of the DataFrame.

    // Here returning the first row
    val firstRow = df.collect()(0)
    // println(firstRow)

    // Here returning the first two rows
    val firstTwoRows = df.collect()(1)
    // println(firstTwoRows)


    import spark.implicits._

    val cols1 = Seq("Name", "Branch")
    val data1 = Seq(
      ("Anusha", "ECE"),
      ("Bindhu", "CSE"),
      ("Chitra", "Mech")
    )

    val df1 = spark.createDataFrame(data1).toDF(cols1: _*)
    // df1.printSchema()
    // df1.show(false)

    // Usage 2 : For String Types
    // Here it will return row & column value of a data frame

    // Here returning the first row value of Name column
    val firstRowNameValue = df1.collect()(0).getAs[String]("Name")
    // println(firstRowNameValue)

    // Here returning the first row value of Branch column
    val firstRowBranchValue = df1.collect()(0).getAs[String]("Branch")
    // println(firstRowBranchValue)

    // Here returning the second row value of Name column
    val secondRowNameValue = df1.collect()(1).getAs[String]("Name")
    // println(secondRowNameValue)

    // Here returning the second row value of Branch column
    val secondRowBranchValue = df1.collect()(1).getAs[String]("Branch")
    // println(secondRowBranchValue)


    import spark.implicits._

    val cols2 = Seq("Marks","Rank")
    val data2 = Seq(
      (100, 4),
      (95, 5),
      (90, 6)
    )

    val df2 = spark.createDataFrame(data2).toDF(cols2: _*)
    // df2.printSchema()
    // df2.show(false)

    // Usage 3 : For Integer Types
    // Here it will return row & column value of a data frame

    // Here returning the first row and first column value
    val firstRowMarksValue = df2.collect()(0).getInt(0)
    // println(firstRowMarksValue)

    // Here returning the first row and second column value
    val firstRowRankValue = df2.collect()(0).getInt(1)
    // println(firstRowRankValue)

    // Here returning the second row and first column value
    val secondRowMarksValue = df2.collect()(1).getInt(0)
    // println(secondRowMarksValue)

    // Here returning the second row and second column value
    val secondRowRankValue = df2.collect()(1).getInt(1)
    // println(secondRowRankValue)

  }
}
