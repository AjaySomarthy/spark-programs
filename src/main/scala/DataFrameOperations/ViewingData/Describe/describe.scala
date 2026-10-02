package DataFrameOperations.ViewingData.Describe

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object describe {
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

    // df.describe()

  }
}

