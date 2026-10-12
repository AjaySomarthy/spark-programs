package interviews.exchangeratewindow

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{expr, lead, to_date}

// Exchange rate window program
object ExchangeRateWindow {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("Exchange rate window program").getOrCreate()

    import spark.implicits._

    val inputData = Seq(
      ("INR","USD",0.9,"10 APRIL 2025"),
      ("INR","USD",0.8,"15 APRIL 2025")
    )
    val inputDF = inputData.toDF("src_cur","target_currency","exchange_rate","effective_start_date")
      .withColumn("effective_start_date",to_date($"effective_start_date","dd MMMM yyyy"))
    /*inputDF.printSchema()
    inputDF.show(false)*/

    val windowSpec = Window.partitionBy($"src_cur",$"target_currency").orderBy("effective_start_date")
    val outputDF = inputDF.withColumn("next_start_date",lead($"effective_start_date",1).over(windowSpec))
      .withColumn("effective_end_date",expr("date_sub(next_start_date,1)")).drop("next_start_date")
    /*outputDF.printSchema()
    outputDF.show(false)*/

  }
}
