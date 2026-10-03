package DataFrame_Operations.Actions.First

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object firstProg {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("First action usage").getOrCreate()

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

    // It will print the first row of a data frame
    // The first() action returns the very first row of the DataFrame as a Row object.
    val result = df.first()
    // println(result)

    val firstRow = df.collect()(2)
    println(firstRow)

  }
}
