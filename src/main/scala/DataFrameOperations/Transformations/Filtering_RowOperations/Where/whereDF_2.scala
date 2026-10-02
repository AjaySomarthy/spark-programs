package DataFrameOperations.Transformations.Filtering_RowOperations.Where

import org.apache.log4j._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.col

object whereDF_2 {
Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Where usage in data frames")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
      Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
      Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
      Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
      Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
      Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
    )
    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("languages",ArrayType(StringType))
      .add("state",StringType)
      .add("gender",StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    // df.printSchema()
    // df.show(false)

    // Type 1 :
    val df1 = df.where("state == 'NY'")
    // df1.show(false)

    // Type 2 :
    val df2 = df.where(col("state") === "OH")
    // df2.show(false)

    // Type 3 :
    val df3 = df.where(df("gender") === "M")
    // df3.show(false)

    val df4 = df.where(df("state") === "OH" && df("gender") === "M")
    // df4.show(false)

    val df5 = df.where(array_contains(df("languages"),"Java"))
    // df5.show(false)

    val df6 = df.where(df("name.lastname") === "Williams")
    // df6.show(false)

  }
}
