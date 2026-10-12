package dataframeoperations.transformations.partitioning

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// Performing coalesce() on Spark DataFrame
object Coalesce {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("Coalescing a DataFrame").getOrCreate()


    // Get input DataFrame from repartition.scala
    val inputDF = Repartition.getInputDF(spark)
    /*inputDF.printSchema()
    inputDF.show(30)

    // Finding the number of partitions of inputDF
    println(s"Number of partitions of inputDF : ${inputDF.rdd.getNumPartitions}")*/


    // Increasing the number of partitions to 20 using repartitioning
    val repartitionedDF = inputDF.repartition(20)
    /*repartitionedDF.printSchema()
    repartitionedDF.show(30)

    // Finding the number of partitions after repartitioning
    println(s"Number of partitions after repartitioning : ${repartitionedDF.rdd.getNumPartitions}")*/


    // Decreasing the repartitionedDF partitions to 10 using coalescing
    val coalescedDF = repartitionedDF.coalesce(10)
    /*coalescedDF.printSchema()
    coalescedDF.show(30)

    // Finding the number of partitions after coalescing
    println(s"Number of partitions after coalescing : ${coalescedDF.rdd.getNumPartitions}")*/

    spark.stop()

  }

}
