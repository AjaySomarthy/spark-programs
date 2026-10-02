package DataFrameOperations.Transformations.Combining.Union

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object unionWithCsvFile {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Union Programme").getOrCreate()

    val employeesOneDF = spark.read.option("header","true").option("Infer","Schema")
      .csv("C:\\Spark_Sample_Files\\Union\\employeesOne.csv")
    // employeesOneDF.printSchema()
    // employeesOneDF.show(false)

    // Here Salary is taking as String, so we have to modify it to integer type
    val updatedEmployeesOneDF = employeesOneDF.withColumn("Salary",col("Salary").cast("Integer"))
    // updatedEmployeesOneDF.printSchema()
    // updatedEmployeesOneDF.show(false)

    val employeesTwoDF = spark.read.option("header", "true").option("Infer", "Schema")
      .csv("C:\\Spark_Sample_Files\\Union\\employeesTwo.csv")
    // employeesTwoDF.printSchema()
    // employeesTwoDF.show(false)

    // Here Salary is taking as String, so we have to modify it to integer type
    val updatedEmployeesTwoDF = employeesTwoDF.withColumn("Salary", col("Salary").cast("Integer"))
    // updatedEmployeesTwoDF.printSchema()
    // updatedEmployeesTwoDF.show(false)

    val employeesUnionDF = updatedEmployeesOneDF.union(updatedEmployeesTwoDF)
    // employeesUnionDF.printSchema()
    // employeesUnionDF.show()

    val employeesUnionDistinctDF = updatedEmployeesOneDF.union(updatedEmployeesTwoDF).distinct()
    // employeesUnionDistinctDF.printSchema()
    // employeesUnionDistinctDF.show()

    val employeesUnionDropDuplicatesDF = updatedEmployeesOneDF.union(updatedEmployeesTwoDF).dropDuplicates()
    // employeesUnionDropDuplicatesDF.printSchema()
    // employeesUnionDropDuplicatesDF.show()


    // Using Spark SQL
    updatedEmployeesOneDF.createOrReplaceTempView("EmpOne")
    updatedEmployeesTwoDF.createOrReplaceTempView("EmpTwo")
    val empUnionDF = spark.sql("select * from EmpOne union all select * from EmpTwo")
    // empUnionDF.printSchema()
    // empUnionDF.show(false)

    val empUnionDistinctDF = spark.sql("select * from EmpOne union select * from EmpTwo")
    // empUnionDistinctDF.printSchema()
    // empUnionDistinctDF.show(false)

  }

}
