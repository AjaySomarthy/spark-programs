package DataFrame_Operations.Actions.Reduce

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object reduceProg {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reduce action usage").getOrCreate()

    import spark.implicits._

    val data = Seq(10, 20, 30)

    val df = data.toDF()
    // df.printSchema()
    // df.show(false)

    // finding the sum of all the rows of data frame using reduce() method
    val sumValue: Int = df.as[Int].rdd.reduce((a, b) => a + b)
    // println(s"Total Sum is : $sumValue")

    // In dataframes rather than converting to RDD to find sum of all the rows, we use built-in function agg()

    // finding the sum of all the rows of data set using reduce() method
    val data1 = Seq(10, 20, 30, 40)
    val ds = data1.toDS()
    val sumValue1: Int = ds.reduce((a, b) => a + b)
    // println(sumValue1)

  }
}
