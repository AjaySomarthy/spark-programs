package DataFrame_Operations.StructType

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object structTypeDF {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Struct Type DF")
      .getOrCreate()

    val data = Seq(
      Row(Row("Anusha", "Patel"), "ECE", 95),
      Row(Row("Bindhu", "Reddy"), "CSE", 90),
      Row(Row("Chitra", "Naidu"), "ECE", 85),
      Row(Row("Divya", "Chowdary"), "Mech", 80),
      Row(Row("Eesha", "Naidu"), "Civil", 92)
    )
    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("lastname",StringType))
      .add("Branch",StringType)
      .add("Marks",IntegerType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    // df.printSchema()
    // df.show(false)

    val rdd = spark.sparkContext.parallelize(data)
    val df2 = spark.createDataFrame(rdd,schema)
    // df2.printSchema()
    // df2.show(false)

  }
}
