package DataFrame_Operations.Transformations.Reshaping.Pivot

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object pivotDfWithSampleData {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={
    var spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Creating Pivot DF with Sample Data")
      .getOrCreate()


    // Finding pivotDF with sample data
    val cols = Seq("Product", "Country", "Amount")
    val data = Seq(
      ("Pen", "India", 10),
      ("Pen", "India", 15),
      ("Pen", "USA", 20),
      ("Pen", "China", 5),
      ("Book", "USA", 100),
      ("Book", "China", 80),
      ("Book", "Japan", 60),
      ("Chocolate", "India", 5),
      ("Chocolate", "Japan", 2)
    )
    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.printSchema()
    // df.show(false)

    val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    // pivotDF.printSchema()
    // pivotDF.show(false)


    // Finding the Pivot by reading the CSV file into DF
    // df.write.option("header","true").option("Infer","Schema").csv("C:\\Spark_Sample_Files\\Pivot\\productsFile.csv")
    val productsFileDF = spark.read.option("header","true")
      .option("Infer","Schema").csv("C:\\Spark_Sample_Files\\Pivot\\productsFile.csv")
    // productsFileDF.printSchema()
    // productsFileDF.show(false)

    // Here its reading Amount column as String type so we have to convert it to Integer type
    val updatedProductFileDF = productsFileDF.withColumn("Amount",col("Amount").cast("Integer"))
    // updatedProductFileDF.printSchema()
    // updatedProductFileDF.show(false)

    val productFilePivotDF = updatedProductFileDF.groupBy("Product").pivot("Country").sum("Amount")
    // productFilePivotDF.printSchema()
    // productFilePivotDF.show(false)

  }

}
