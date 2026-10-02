package Equal_DoubleEqual_TrippleEqual

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// Difference among = , == and ===
object equal_doubleEqual_trippleEqual {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("usage of = , == and ===").getOrCreate()

    // 1) = usage
    var a = 3
    // println(a)

    val myList1 = List(1,2,3)
    // println(myList1)

    val df = spark.read.option("header","true").csv("C:\\Spark_Sample_Files\\Union\\employeesOne.csv")
    // df.show(false)


    // 2) == usage
    val b = 7
    val c = 7
    val d = 8
    // println(b == c)
    // println(b == d)


    // 3) === usage
    val cols1 = Seq("ID", "Name", "Marks")
    val data1 = Seq(
      (101, "Anusha", 95),
      (201, "Bindhu", 35),
      (301, "Chitra", 20)
    )

    val df1 = spark.createDataFrame(data1).toDF(cols1: _*)
    // df1.printSchema()
    // df1.show(false)

    val cols2 = Seq("ID", "Rank")
    val data2 = Seq(
      (101, 1),
      (201, 2),
      (301, 3)
    )

    val df2 = spark.createDataFrame(data2).toDF(cols2: _*)
    // df2.printSchema()
    // df2.show(false)

    val df3 = df1.filter(col("Marks") === 35)
    // df3.show(false)

    val df4 = df1.join(df2, df1("ID") === df2("ID"), "Inner")
    // df4.show(false)


    // =, ==, === in one example
    val cols5 = Seq("Name", "Marks")
    val data5 = Seq(
      ("Anusha", 95),
      ("Bindhu", 80),
      ("Chitra", 45),
      ("Divya", 25)
    )

    val df5 = spark.createDataFrame(data5).toDF(cols5: _*)
    // df5.printSchema()
    // df5.show(false)

    val df6 = df5.filter("Name = 'Anusha'")
    // df6.show(false)
    val df7 = df5.filter("Name == 'Bindhu'")
    // df7.show(false)
    val df8 = df5.filter(col("Name") === "Chitra")
    // df8.show(false)

  }
}
