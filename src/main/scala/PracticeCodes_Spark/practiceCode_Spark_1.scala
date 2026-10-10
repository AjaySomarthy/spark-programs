package PracticeCodes_Spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object practiceCode_Spark_1 {
   Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark : SparkSession = SparkSession.builder()
      .master("local[1]").appName("Union example in Spark with Scala")
      .getOrCreate()

    val cols1 = Seq("ID", "Name", "Marks")
    val data1 = Seq(
      (101, "Anusha", 99),
      (101, "Anusha", 99),
      (201, "Bindhu", 80),
      (301, "Chitra", 70)
    )
    val df1 = spark.createDataFrame(data1).toDF(cols1:_*)
    // df1.printSchema()
    // df1.show(false)

    val cols2 = Seq("ID", "Name", "Marks")
    val data2 = Seq(
      (201, "Bindhu", 80),
      (401, "Divya", 60),
      (501, "Eesha", 50)
    )
    val df2 = spark.createDataFrame(data2).toDF(cols2: _*)
    // df2.printSchema()
    // df2.show(false)

    val unionDF =df1.union(df2)
    // unionDF.printSchema()
    // unionDF.show(false)

    val distinctUnionDF = df1.union(df2).distinct()
    // distinctUnionDF.printSchema()
    // distinctUnionDF.show(false)

    val dropDuplicatesUnionDF = df1.union(df2).dropDuplicates()
    // dropDuplicatesUnionDF.printSchema()
    // dropDuplicatesUnionDF.show(false)

    // df1.write.option("header","true").option("inferSchema","true").csv("C:\\Spark_Sample_Files\\Union\\first.csv")
    // df2.write.option("header","true").option("inferSchema","true").csv("C:\\Spark_Sample_Files\\Union\\second.csv")

    val firstDF = spark.read.option("header","true").option("inferSchema","true")
      .csv("C:\\Spark_Sample_Files\\Union\\first.csv")
    /*firstDF.printSchema()
    firstDF.show(false)*/

    val secondDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:\\Spark_Sample_Files\\Union\\second.csv")
   /* secondDF.printSchema()
    secondDF.show(false)*/

    val unionCSVDF = firstDF.union(secondDF)
    unionDF.printSchema()
    unionDF.show(false)

    val unionDistinctCSVDF = firstDF.union(secondDF).distinct()
    unionDistinctCSVDF.printSchema()
    unionDistinctCSVDF.show(false)

    val unionDropDuplicatesCSVDF = firstDF.union(secondDF).dropDuplicates()
    unionDropDuplicatesCSVDF.printSchema()
    unionDropDuplicatesCSVDF.show(false)


  }
}
