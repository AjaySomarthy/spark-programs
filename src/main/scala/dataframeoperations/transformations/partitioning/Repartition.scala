package dataframeoperations.transformations.partitioning

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

// Performing repartition() on Spark DataFrame
object Repartition {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def getInputDF(spark : SparkSession) : DataFrame = {
    val inputDF = spark.read.option("header","true").option("inferSchema","true")
      .csv("src/main/resources/dataframeoperations/transformations/repartitioncoalesce/students_data.csv")

    inputDF
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Rpartitioning a DataFrame").getOrCreate()


    val inputDF = getInputDF(spark)
    /*inputDF.printSchema()
    inputDF.show(30)*/

    // Finding the number of partitions of inputDF
    /*println(s"Number of partitions of inputDF : ${inputDF.rdd.getNumPartitions}")*/


    // Increasing the number of partitions to 20 using repartitioning
    val repartitionedDF = inputDF.repartition(20)
    /*repartitionedDF.printSchema()
    repartitionedDF.show(30)*/

    // Finding the number of partitions after repartitioning
    /*println(s"Number of partitions after repartitioning : ${repartitionedDF.rdd.getNumPartitions}")*/


    // Decreasing the number of partitions to 15 using repartitioning
    val repartitionedDF1 = repartitionedDF.repartition(15)
    /*repartitionedDF1.printSchema()
    repartitionedDF1.show(30)*/

    // Finding the number of partitions after repartitioning
    /*println(s"Number of partitions after repartitioning : ${repartitionedDF1.rdd.getNumPartitions}")*/

    spark.stop()

  }
}
