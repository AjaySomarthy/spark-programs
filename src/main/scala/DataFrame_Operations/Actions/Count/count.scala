package DataFrame_Operations.Actions.Count

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// count() usage in Spark Data Frames
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
    /*df.printSchema()
    df.show(false)*/

    // Finding number of rows in a data frame
    /*println("Number of rows in a data frame is :")
    println(df.count())*/

    // Finding number of distinct rows in a data frame
    /*println("Number of distinct rows in a data frame is :")
    println(df.distinct().count())*/


    // distinct df vs distinct number of rows
    // df.distinct() is a data frame with distinct rows
    // df.distinct().count() is a value, i.e it tells how many number of distinct rows existed in a data frame
    /*df.distinct().show(false)
    println(df.distinct().count())*/


    // observe these
    /*println(df.count())                   // it returns a value
    println(df.count().getClass)          // it returns it's type which is long
    println(df.count().toInt)             // it converts the value to int and prints that value
    println(df.count().toInt.getClass)*/    // it converts the value to int and prints that type

    // Note : With out println() method, these prints nothing
    /*df.count()
    df.count().getClass()
    df.count().toInt
    df.count().toInt.getClass*/


    // Task : Find how many number of students are having marks greater than 75
    // we have to exclude duplicates here, so
    /*val df1 = df.distinct().where(col("Marks") >= 75)
    df1.show(false)*/

  }
}
