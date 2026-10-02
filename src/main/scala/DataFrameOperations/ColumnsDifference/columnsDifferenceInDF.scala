package DataFrameOperations.ColumnsDifference

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,max,min,desc,dense_rank}
import org.apache.spark.sql.expressions.Window

object columnsDifferenceInDF {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Finding the difference of columns").getOrCreate()

    val cols = Seq("Product", "JanuarySales", "FebraurySales")
    val data = Seq(
      ("Apple", 100, 50),
      ("Banana", 200, 40),
      ("Carrot", 300, 30)
    )
    val df = spark.createDataFrame(data).toDF(cols: _*)
    // df.printSchema()
    // df.show(false)

    val diffDF = df.withColumn("DifferenceSales",col("JanuarySales")-col("FebraurySales"))
      .withColumn("TotalSales",col("JanuarySales")+col("FebraurySales"))
      .withColumn("MultipliedSales",col("JanuarySales")*col("FebraurySales"))
      .withColumn("DividedSales",col("JanuarySales")/col("FebraurySales"))
    // diffDF.printSchema()
    // diffDF.show(false)

    val percentageDifferenceDF = df.withColumn("PercentageDifferenceSales",((col("JanuarySales")-col("FebraurySales"))/col("JanuarySales"))*100)
      .withColumn("PercentageTotalSales",((col("JanuarySales")+col("FebraurySales"))/col("JanuarySales"))*100)
      .withColumn("PercentageMultipliedSales",((col("JanuarySales")*col("FebraurySales"))/col("JanuarySales"))*100)
      .withColumn("PercentageDividedSales",((col("JanuarySales")/col("FebraurySales"))/col("JanuarySales"))*100)
    // percentageDifferenceDF.printSchema()
    // percentageDifferenceDF.show(false)

    val data1 = Seq(
      ("Apple", 100),
      ("Apple", 50),
      ("Apple", 20),
      ("Banana", 200),
      ("Banana", 60),
      ("Banana", 50),
      ("Carrot", 300),
      ("Carrot", 80),
      ("Carrot", 90)
    )
    val cols1 = Seq("ProductName", "SalesPerDay")
    val df1 = spark.createDataFrame(data1).toDF(cols1: _*)
    //df1.printSchema()
    //df1.show(false)

    val DifferenceInSalesDF1 = df1.groupBy("ProductName").agg(
      (max("SalesPerDay")-min("SalesPerDay")).as("DifferenceInSales")
    )
    // DifferenceInSalesDF1.printSchema()
    // DifferenceInSalesDF1.show(false)

    val data2 = Seq(
      ("Apple", 100),
      ("Apple", 50),
      ("Apple", 20),
      ("Banana", 200),
      ("Banana", 60),
      ("Banana", 50),
      ("Carrot", 300),
      ("Carrot", 80),
      ("Carrot", 90)
    )
    val cols2 = Seq("ProductName", "SalesPerDay")
    val df2 = spark.createDataFrame(data2).toDF(cols2: _*)
    //df2.printSchema()
    //df2.show(false)

    // Top 2 sales from df product wise
    val windowSpec = Window.partitionBy("ProductName").orderBy(desc("SalesPerDay"))
    val windowFunctionDF = df2.withColumn("DenseRank",dense_rank().over(windowSpec))
      .where(col("DenseRank") <= 2)
    // windowFunctionDF.printSchema()
    windowFunctionDF.show(false)

    val resultDF = windowFunctionDF.groupBy("ProductName").agg(
      (((max("SalesPerDay")-min("SalesPerDay"))/max("SalesPerDay"))*100).as("DifferenceInSalesPercentage")
    )
    // resultDF.printSchema()
    // resultDF.show(false)

  }
}
