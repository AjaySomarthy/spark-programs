package DataFrameOperations.ViewingData.Show

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object show {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Collect action usage").getOrCreate()

    val cols = Seq("Name","Marks")
    val data = Seq(
      ("Anusha",90),
      ("Anusha",90),
      ("Bindhu",85),
      ("Chitra",80),
      ("Divya",70)
    )

    val df = spark.createDataFrame(data).toDF(cols:_*)

    // df.show()
    // df.show(false)

    val cols1 = Seq("Name", "Occupation")
    val data1 = Seq(
      ("Anusha", "Teacher"),
      ("Bindhu", "IT Software Employee"),
      ("Chitra", "She studied MBBS and she is a Doctor")
    )

    val df1 = spark.createDataFrame(data1).toDF(cols1: _*)

    // df1.show()
    // df1.show(true)
    // df1.show(false)

    // observe the difference from the above

  }
}

