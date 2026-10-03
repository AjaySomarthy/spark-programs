package DataFrame_Operations.Transformations.Grouping.GroupBy

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Group By operation on multiple columns programme
object groupByWithTwoColumns {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Group By operation on multiple columns")
      .getOrCreate()
    import spark.implicits._

    val simpleData = Seq(
      ("Anusha","ECE","Hyd",20500),
      ("Bindhu","ECE","Del",35000),
      ("Chitra","ECE","Hyd",21500),
      ("Divya","ECE","Kol",40500),
      ("Eesha","CSE","Hyd",50000),
      ("Fathima","CSE","Del",10500),
      ("Ganga","CSE","Del",15000),
      ("Harika","CSE","Kol",65000)
    )
    val df = simpleData.toDF("name","department","area","salary")
    // df.show(false)

    val df1 = df.groupBy("department","area").agg(
      min("salary").as("MinSalary"),
      max("salary").as("MaxSalary"),
      sum("salary").as("SumSalary"),
      avg("salary").as("AvgSalary"),
      mean("salary").as("MeanSalary"),
      count("salary").as("TotalCount")
    )
    // df1.show(false)

    df.createOrReplaceTempView("Employees")
    spark.sql("select department, area, min(salary) as MinSalary, max(salary) as MaxSalary, " +
      "sum(salary) as SumSalary, avg(salary) as AvgSalary, mean(salary) as MeanSalary, count(salary) as TotalCount " +
      "from Employees group by department, area").show(false)
  }

}
