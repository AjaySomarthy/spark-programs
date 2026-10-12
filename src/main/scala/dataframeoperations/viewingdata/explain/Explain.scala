package dataframeoperations.viewingdata.explain

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Explain {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("explain() usage").getOrCreate()

    val cols = Seq("Name","Marks")
    val data = Seq(
      ("Anusha",90),
      ("Anusha",90),
      ("Bindhu",85),
      ("Chitra",80),
      ("Divya",70)
    )

    val df = spark.createDataFrame(data).toDF(cols:_*)

    /*df.explain()
    df.explain(false)
    df.explain(true)*/

  }
}

