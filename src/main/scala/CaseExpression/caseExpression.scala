package CaseExpression

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,when}

// Spark DataFrame Conditional expression using when() and otherwise()
object caseExpression {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={
    val spark : SparkSession = SparkSession.builder()
      .master("local[1]").appName("Conditional Column addition using case expression")
      .getOrCreate()

    // Example 1 :
    val cols = Seq("Name","Age")
    val data = Seq(
      ("Anusha",23),
      ("Bindhu",5),
      ("Chitra",18),
      ("Divya",17),
      ("Eesha",13)
    )

    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.printSchema()
    // df.show(false)

    val caseExpressionDF = df.withColumn("Age_Status",
      when(col("Age") >= 18, "Major")
        .when(col("Age") >= 13, "Teenager")
        .otherwise("Child"))
    // caseExpressionDF.printSchema()
    // caseExpressionDF.show(false)

    df.createOrReplaceTempView("Persons")
    val personsDF = spark.sql("select Name, Age, " +
      "case " +
      "when age >= 18 then 'Major' " +
      "when age >=13 then 'Teenager' " +
      "else 'Child' " +
      "end as Age_Status " +
      "from Persons")
    // personsDF.show(false)


    // Example 2 :
    val cols1 = Seq("Name", "Marks")
    val data1 = Seq(
      ("Anusha", 95),
      ("Bindhu", 22),
      ("Chitra", 35),
      ("Divya", 84),
      ("Eesha", 2),
      ("Fathima", 0),
      ("Ganga", -3),
      ("Harika", 20456)
    )

    val df1 = spark.createDataFrame(data1).toDF(cols1: _*)
    // df1.printSchema()
    // df1.show(false)

    val df2 = df1.withColumn("Marks Status",
      when(col("Marks") > 35, "First Class")
        .when(col("Marks") === 35, "Pass")
        .otherwise("Fail")
    )

    // df2.printSchema()
    // df2.show(false)

  }

}
