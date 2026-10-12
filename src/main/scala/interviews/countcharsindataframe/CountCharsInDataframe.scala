package interviews.countcharsindataframe

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, udf}

// Counting Characters in a DataFrame
object CountCharsInDataframe {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("Count Chars in DataFrame").getOrCreate()

    import spark.implicits._

    val data = Seq("abcde","aabcd","aaabc")
    val cols = Seq("chars")

    val df = data.toDF(cols:_*)
    /*df.printSchema()
    df.show(false)*/

    val countA = udf((str : String) => str.count(_ == 'a'))
    val resultDF = df.withColumn("a_chars",concat($"chars",countA($"chars")))
    /*resultDF.printSchema()
    resultDF.show(false)*/

  }
}
