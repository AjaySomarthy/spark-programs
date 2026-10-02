package DataFrameOperations.Repartitioning_Coalescing

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// repartitioning on a data frame
object select {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("repartitioning on a data frame")
      .getOrCreate()

    import spark.implicits._

    // Sample data creation with initial partitions
    val data = Seq(
      ("Alice", "HR", 3000),
      ("Bob", "IT", 4000),
      ("Cathy", "HR", 3500),
      ("David", "Finance", 4500),
      ("Eva", "IT", 5000)
    )

    // Create a DataFrame with 2 initial partitions
    val initialDF = spark.sparkContext.parallelize(data, 2).toDF("name", "department", "salary")

    println(s"Initial number of partitions: ${initialDF.rdd.getNumPartitions}")

    // 1. Increase or decrease partitions using repartition (triggers a full shuffle)
    val repartitionedDF = initialDF.repartition(4)
    println(s"Number of partitions after repartition(4): ${repartitionedDF.rdd.getNumPartitions}")

    // 2. Repartition by column (co-locates rows with the same department into the same partition)
    val colRepartitionedDF = initialDF.repartition(2, $"department")
    println(s"Number of partitions after repartition by column: ${colRepartitionedDF.rdd.getNumPartitions}")

    // Stop Spark session
    spark.stop()

  }
}
