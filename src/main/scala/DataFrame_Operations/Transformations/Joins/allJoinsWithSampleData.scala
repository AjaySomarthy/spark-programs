package DataFrame_Operations.Transformations.Joins

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// Inner Join, Left Join, Right Join, Full Join, Left Semi Join, Left Anti Join Programme
object allJoinsWithSampleData {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark : SparkSession = SparkSession.builder()
      .master("local[1]").appName("All Joins Programme")
      .getOrCreate()


    // All Joins with Sample data
    val empCols = Seq("emp_name", "emp_dept_id", "emp_salary")
    val empData = Seq(
      ("Anusha", 101, 25000),
      ("Bindhu", 201, 30500),
      ("Chitra", 301, 40800),
      ("Divya", 501, 20500),
      ("Eesha", 601, 55000),
      ("Fathima", 101, 62500)
    )
    val empDF = spark.createDataFrame(empData).toDF(empCols:_*)
    // empDF.printSchema()
    // empDF.show(false)

    val deptCols = Seq("dept_name", "dept_id")
    val deptData = Seq(
      ("IT", 101),
      ("Sales", 201),
      ("Marketing", 301),
      ("Finance", 401)
    )
    val deptDF = spark.createDataFrame(deptData).toDF(deptCols: _*)
    // deptDF.printSchema()
    // deptDF.show(false)

    val innerJoinDF = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"),"inner")
    // innerJoinDF.printSchema()
    //innerJoinDF.show(false)

    val leftJoinDF = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left")
    // leftJoinDF.printSchema()
    // leftJoinDF.show(false)

    val rightJoinDF = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "right")
    // rightJoinDF.printSchema()
    // rightJoinDF.show(false)

    val fullJoinDF = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full")
    // fullJoinDF.printSchema()
    // fullJoinDF.show(false)

    val leftSemiJoinDF = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftsemi")
    // leftSemiJoinDF.printSchema()
    // leftSemiJoinDF.show(false)

    val leftAntiJoinDF = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftanti")
    // leftAntiJoinDF.printSchema()
    // leftAntiJoinDF.show(false)


    // Writing the above DFs into CSV and reading them into DF and finding all Joins
    // empDF.write.option("header","true").option("Infer","Schema").csv("C:\\Spark_Sample_Files\\Joins\\empFile.csv")
    // deptDF.write.option("header","true").option("Infer","Schema").csv("C:\\Spark_Sample_Files\\Joins\\deptFile.csv")
    val empFileDF = spark.read.option("header","true").option("Infer","Schema")
      .csv("C:\\Spark_Sample_Files\\Joins\\empFile.csv")
    // empFileDF.printSchema()
    // empFileDF.show(false)

    val deptFileDF = spark.read.option("header","true").option("Infer","Schema")
      .csv("C:\\Spark_Sample_Files\\Joins\\deptFile.csv")
    // deptFileDF.printSchema()
    // deptFileDF.show(false)

    val innerJoinFileDF = empFileDF.join(deptFileDF, empFileDF("emp_dept_id") === deptFileDF("dept_id"), "inner")
    // innerJoinFileDF.printSchema()
    innerJoinFileDF.show(false)

    val leftJoinFileDF = empFileDF.join(deptFileDF, empFileDF("emp_dept_id") === deptFileDF("dept_id"), "left")
    // leftJoinFileDF.printSchema()
    leftJoinFileDF.show(false)

    val rightJoinFileDF = empFileDF.join(deptFileDF, empFileDF("emp_dept_id") === deptFileDF("dept_id"), "right")
    // rightJoinFileDF.printSchema()
    rightJoinFileDF.show(false)

    val fullJoinFileDF = empFileDF.join(deptFileDF, empFileDF("emp_dept_id") === deptFileDF("dept_id"), "full")
    // fullJoinFileDF.printSchema()
    fullJoinFileDF.show(false)

    val leftSemiJoinFileDF = empFileDF.join(deptFileDF, empFileDF("emp_dept_id") === deptFileDF("dept_id"), "leftsemi")
    // leftSemiJoinFileDF.printSchema()
    leftSemiJoinFileDF.show(false)

    val leftAntiJoinFileDF = empFileDF.join(deptFileDF, empFileDF("emp_dept_id") === deptFileDF("dept_id"), "leftanti")
    // leftAntiJoinFileDF.printSchema()
    leftAntiJoinFileDF.show(false)

  }
}
