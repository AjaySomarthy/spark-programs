package DataFrame_Operations.Transformations.Joins

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object allJoinsWithCsvFile {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("All Joins Programme")
      .getOrCreate()

    // Finding all Joins from CSV files
    // empDF.write.option("header","true").option("Infer","Schema").csv("C:\\Spark_Sample_Files\\Joins\\empFile.csv")
    // deptDF.write.option("header","true").option("Infer","Schema").csv("C:\\Spark_Sample_Files\\Joins\\deptFile.csv")
    val employeesDataDF = spark.read.option("header", "true").option("Infer", "Schema")
      .csv("C:\\Spark_Sample_Files\\Joins\\employeesData.csv")
    // employeesDataDF.printSchema()
    // employeesDataDF.show(false)

    val departmentDataDF = spark.read.option("header", "true").option("Infer", "Schema")
      .csv("C:\\Spark_Sample_Files\\Joins\\departmentData.csv")
    // departmentDataDF.printSchema()
    // departmentDataDF.show(false)

    val innerJoinDataDF = employeesDataDF
      .join(departmentDataDF, employeesDataDF("emp_dept_id") === departmentDataDF("dept_id"), "inner")
    // innerJoinDataDF.printSchema()
    // innerJoinDataDF.show(false)

    val leftJoinDataDF = employeesDataDF
      .join(departmentDataDF, employeesDataDF("emp_dept_id") === departmentDataDF("dept_id"), "left")
    // leftJoinDataDF.printSchema()
    // leftJoinDataDF.show(false)

    val rightJoinDataDF = employeesDataDF
      .join(departmentDataDF, employeesDataDF("emp_dept_id") === departmentDataDF("dept_id"), "right")
    // rightJoinDataDF.printSchema()
    // rightJoinDataDF.show(false)

    val fullJoinDataDF = employeesDataDF
      .join(departmentDataDF, employeesDataDF("emp_dept_id") === departmentDataDF("dept_id"), "full")
    // fullJoinDataDF.printSchema()
    // fullJoinDataDF.show(false)

    val leftSemiJoinDataDF = employeesDataDF
      .join(departmentDataDF, employeesDataDF("emp_dept_id") === departmentDataDF("dept_id"), "leftsemi")
    // leftSemiJoinDataDF.printSchema()
    // leftSemiJoinDataDF.show(false)

    val leftAntiJoinDataDF = employeesDataDF
      .join(departmentDataDF, employeesDataDF("emp_dept_id") === departmentDataDF("dept_id"), "leftanti")
    // leftAntiJoinDataDF.printSchema()
    // leftAntiJoinDataDF.show(false)


    //  Finding all joins using Spark SQL
    employeesDataDF.createOrReplaceTempView("employees")
    departmentDataDF.createOrReplaceTempView("department")
    val innerJoinEmployeesDF = spark.sql("select * from employees e inner join department d on e.emp_dept_id == d.dept_id")
    // innerJoinEmployeesDF.printSchema()
    // innerJoinEmployeesDF.show(false)

    val leftJoinEmployeesDF = spark.sql("select * from employees e left join department d on e.emp_dept_id == d.dept_id")
    // leftJoinEmployeesDF.printSchema()
    // leftJoinEmployeesDF.show(false)

    val rightJoinEmployeesDF = spark.sql("select * from employees e right join department d on e.emp_dept_id == d.dept_id")
    // rightJoinEmployeesDF.printSchema()
    // rightJoinEmployeesDF.show(false)

    val fullJoinEmployeesDF = spark.sql("select * from employees e full join department d on e.emp_dept_id == d.dept_id")
    // fullJoinEmployeesDF.printSchema()
    // fullJoinEmployeesDF.show(false)

    val leftSemiJoinEmployeesDF = spark.sql("select * from employees e left semi join department d on e.emp_dept_id == d.dept_id")
    // leftSemiJoinEmployeesDF.printSchema()
    // leftSemiJoinEmployeesDF.show(false)

    val leftAntiJoinEmployeesDF = spark.sql("select * from employees e left anti join department d on e.emp_dept_id == d.dept_id")
    // leftAntiJoinEmployeesDF.printSchema()
    // leftAntiJoinEmployeesDF.show(false)

  }
}
