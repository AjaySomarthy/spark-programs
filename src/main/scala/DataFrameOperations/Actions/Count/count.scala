package DataFrameOperations.Actions.Count

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object count {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Count action usage").getOrCreate()

    val cols = Seq("Name","Marks")
    val data = Seq(
      ("Anusha",90),
      ("Anusha",90),
      ("Bindhu",85),
      ("Chitra",80),
      ("Divya",70)
    )
    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.printSchema()
    // df.show(false)

    // println("Number of rows in a data frame is :")
    // println(df.count())

    // here count acts as an action

    // println("Number of distinct rows in a data frame is :")
    // println(df.distinct().count())

    val df1 = df.distinct()
    // df1.printSchema()
    // df1.show(false)

    // println(df1.count())

    // How many number of students are having marks greater than 75
    val df2 = df.distinct().where(col("Marks") >= 75)
    // println(df2.count())

  }
}
