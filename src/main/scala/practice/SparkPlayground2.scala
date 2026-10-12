package practice

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// Spark Playground2 for Spark Scala programs
object SparkPlayground2 {
   Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark : SparkSession = SparkSession.builder()
      .master("local[1]").appName("Repartitioning a Data Frame")
      .getOrCreate()

    import spark.implicits._

    /*val cols = Seq("ID","Name","Branch","Marks")
    val data = Seq(
      (101, "Anusha", "ECE", 90),
      (201, "Bindhu", "ECE", 80),
      (301, "Chitra", "ECE", 70),
      (401, "Divya", "Mech", 60),
      (501, "Eesha", "Mech", 50)
    )

    val df = spark.createDataFrame(data).toDF(cols:_*)
    df.printSchema()
    df.show(false)
    println(s"Number of Partitions : ${df.rdd.getNumPartitions}")

    val repartitionedDF = df.repartition(4)
    repartitionedDF.printSchema()
    repartitionedDF.show(false)
    println(s"Number of partitions after repartitioning : ${repartitionedDF.rdd.getNumPartitions}")

    val colRepartitionedDF = df.repartition(3, $"Branch")
    colRepartitionedDF.printSchema()
    colRepartitionedDF.show(false)
    println(s"Number of partitions after column wise repartitioning : ${colRepartitionedDF.rdd.getNumPartitions}")

    // Stop Spark session
    spark.stop()*/

    val df = spark.read.option("header","true").option("inferSchema","true")
      .csv("src/main/resources/students.csv")
    df.printSchema()
    df.show(false)

    // df.write.option("header","true").csv("src/main/resources/file1.csv")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/file1.csv")
    df1.printSchema()
    df1.show(false)

  }
}
