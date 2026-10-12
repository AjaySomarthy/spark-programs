package dataframeoperations.transformations.partitioning

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// Performing repartition() and coalesce() on a DataFrame
object RepartitionCoalesce {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Repartitioning and Coalescing on a DataFrame")
      .getOrCreate()

    val inputDF = spark.read.option("header","true").option("inferSchema","true")
      .csv("src/main/resources/dataframeoperations/transformations/repartitioncoalesce/students_data.csv")
    /*inputDF.printSchema()
    inputDF.show(30)*/

    // Finding the number of partitions of input Data Frame
    /*println(s"Number of partitions of input df : ${inputDF.rdd.getNumPartitions}")*/


    // Increasing the number of partitions by repartitioning
    val repartitionedDF = inputDF.repartition(20)
    /*repartitionedDF.printSchema()
    repartitionedDF.show(false)*/

    // Finding the number of partitions after repartitioning
    /*println(s"Number of partitions after repartitioning : ${repartitionedDF.rdd.getNumPartitions}")*/


    // Decreasing the number of partitions by repartitioning
    val repartitionedDF1 = repartitionedDF.repartition(15)
    /*repartitionedDF1.printSchema()
    repartitionedDF1.show(false)*/

    // Finding the number of partitions after repartitioning
    /*println(s"Number of partitions after repartitioning : ${repartitionedDF1.rdd.getNumPartitions}")*/


    // Increasing the number of partitions by column using repartitioning
    val colRepartitionedDF = inputDF.repartition(5, col("Branch"))
    /*colRepartitionedDF.printSchema()
    colRepartitionedDF.show(false)*/

    // Finding the number of partitions after column wise repartitioning
    /*println(s"Number of partitions after column wise repartitioning : ${colRepartitionedDF.rdd.getNumPartitions}")*/


    // Decreasing the number of partitions by coalescing
    val coalescedDF = repartitionedDF.coalesce(10)
    /*coalescedDF.printSchema()
    coalescedDF.show(false)*/

    // Finding the number of partitions after coalescing
    /*println(s"Number of partitions after coalescing : ${coalescedDF.rdd.getNumPartitions}")*/

  }
}
