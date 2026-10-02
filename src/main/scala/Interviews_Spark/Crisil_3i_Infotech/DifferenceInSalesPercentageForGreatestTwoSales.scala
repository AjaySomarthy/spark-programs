package Interviews_Spark.Crisil_3i_Infotech

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc,dense_rank,col,max,min}

object DifferenceInSalesPercentageForGreatestTwoSales {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("Finding the Difference in sales for the latest 2 sales").getOrCreate()

    val data = Seq(
      ("AB","2024-01-03",100000),
      ("AB","2023-01-03",90000),
      ("AB","2022-01-03",80000),
      ("BD","2024-01-03",100000),
      ("BD","2023-01-03",70000),
      ("BD","2022-01-03",85000)
    )
    val cols = Seq("company","date","sales")
    val df = spark.createDataFrame(data).toDF(cols:_*)
    //df.printSchema()
    //df.show(false)

    // Top two sales
    val windowSpec = Window.partitionBy("company").orderBy(desc("date"))
    val greatestTwoDatesDF = df.withColumn("DenseRank",dense_rank().over(windowSpec))
      .where(col("DenseRank")<=2)
    //greatestTwoDatesDF.printSchema()
    //greatestTwoDatesDF.show(false)

    // Top two sales difference percentage
    val DifferenceInPercentageSalesDF = greatestTwoDatesDF.groupBy("company").agg(
      (((max("sales")-min("sales"))/max("sales"))*100).as("DifferenceInPercentageSales")
    )
    //DifferenceInPercentageSalesDF.printSchema()
    DifferenceInPercentageSalesDF.show(false)
  }
}
