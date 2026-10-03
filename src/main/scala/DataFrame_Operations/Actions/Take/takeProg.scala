package DataFrame_Operations.Actions.Take

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object takeProg {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Take action usage").getOrCreate()

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

    // It will print the first 2 rows of a data frame
    // The take(n) action returns an array containing the first n rows of the DataFrame.
    val result = df.take(2)
    // for (i <- result) { println(i) }

  }
}
