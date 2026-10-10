package DataFrame_Operations.Actions.Count

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// count() as an action in Spark Data Frames
object count {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Count action usage in Spark Data Frames").getOrCreate()

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


    // observe the below :
    // In Spark Scala to check the return type we use "value.getClass"
    // println(df.count().getClass)

    // In Spark Scala to convert a value to int we use "value.toInt"
    // println(df.count().toInt)

    // checking the return type name and now we can see it is int
    // println(df.count().toInt.getClass)

    // checking the return type name
    // println(df.count().toInt.getClass.getSimpleName)


    // Task : Find how many number of students are having marks greater than 75
    // we have to exclude duplicates here
    val distinctStudentsCount = df.distinct().where(col("Marks") >= 75).count()
    println(distinctStudentsCount)

  }
}
