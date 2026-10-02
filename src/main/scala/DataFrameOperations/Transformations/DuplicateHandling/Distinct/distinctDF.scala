package DataFrameOperations.Transformations.DuplicateHandling.Distinct

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object distinctDF {
    Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit= {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Distinct DF")
      .getOrCreate()
    import spark.implicits._

    val cols = Seq("Name", "Department", "Salary")
    val data = Seq(
      ("Anusha","IT",25000),
      ("Anusha","IT",25000),
      ("Bindhu","Sales",35000),
      ("Chitra","Marketing",35000),
      ("Divya","Marketing",40500),
      ("Eesha","Marketing",40500)
    )
    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.printSchema()
    // df.show(false)

    val df1 = df.distinct()
    // df1.show(false)

    // println("Distinct Count : "+df1.count())

  }
}
