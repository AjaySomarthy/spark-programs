package DataFrame_Operations.Transformations.DuplicateHandling.DropDuplicates

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object dropDuplicatesDF {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit= {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Drop Duplicates DF")
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

    val df1 = df.dropDuplicates()
    // df1.show(false)

    val df2 = df.dropDuplicates("Department")
    // df2.show(false)

    val df3 = df.dropDuplicates("Department","Salary")
    // df3.show(false)

  }
}
