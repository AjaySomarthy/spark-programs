package DataFrameOperations.Transformations.Combining.Union

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object unionWithSampleData {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Union Programme").getOrCreate()

    // Finding Union from sample data
    val studentsCols1 = Seq("Name","Branch","Marks")
    val studentsData1 = Seq(
      ("Anusha","ECE",95),
      ("Bindhu","CSE",90),
      ("Chitra","IT",65),
      ("Anusha","ECE",95),
      ("Divya","Mech",90)
    )
    val studentsDF1 = spark.createDataFrame(studentsData1).toDF(studentsCols1:_*)
    // studentsDF1.printSchema()
    // studentsDF1.show(false)

    val studentsCols2 = Seq("Name", "Branch", "Marks")
    val studentsData2 = Seq(
      ("Eesha", "Chem", 80),
      ("Fathima", "Civil", 70),
      ("Fathima", "Civil", 70),
      ("Ganga", "EEE", 65),
      ("Harika", "IT", 75)
    )
    val studentsDF2 = spark.createDataFrame(studentsData2).toDF(studentsCols2: _*)
    // studentsDF2.printSchema()
    // studentsDF2.show(false)

    val studentsUnionDF = studentsDF1.union(studentsDF2)
    // studentsUnionDF.printSchema()
    // studentsUnionDF.show(false)

    val studentsUnionDistinctDF = studentsDF1.union(studentsDF2).distinct()
    // studentsUnionDistinctDF.printSchema()
    // studentsUnionDistinctDF.show(false)

             // or
    val studentsUnionDropDuplicatesDF = studentsDF1.union(studentsDF2).dropDuplicates()
    // studentsUnionDropDuplicatesDF.printSchema()
    // studentsUnionDropDuplicatesDF.show(false)


    // Finding Union by reading CSV file into DF
    // studentsDF1.write.option("header","true").option("Infer","Schema").csv("C:\\Spark_Sample_Files\\Union\\firstHalfStudents.csv")
    // studentsDF2.write.option("header", "true").option("Infer", "Schema").csv("C:\\Spark_Sample_Files\\Union\\secondHalfStudents.csv")

    val firstHalfStudentsDF = spark.read.option("header","true").option("Infer","Schema")
      .csv("C:\\Spark_Sample_Files\\Union\\firstHalfStudents.csV")
    // firstHalfStudentsDF.printSchema()
    // firstHalfStudentsDF.show(false)

    val secondHalfStudentsDF = spark.read.option("header", "true").option("Infer", "Schema")
      .csv("C:\\Spark_Sample_Files\\Union\\secondHalfStudents.csV")
    // secondHalfStudentsDF.printSchema()
    // secondHalfStudentsDF.show(false)

    val totalStudentsUnionDF = firstHalfStudentsDF.union(secondHalfStudentsDF)
    // totalStudentsUnionDF.printSchema()
    // totalStudentsUnionDF.show(false)

    val totalStudentsDistinctUnionDF = firstHalfStudentsDF.union(secondHalfStudentsDF).distinct()
    // totalStudentsDistinctUnionDF.printSchema()
    // totalStudentsDistinctUnionDF.show(false)

    val totalStudentsDropDuplicatesUnionDF = firstHalfStudentsDF.union(secondHalfStudentsDF).dropDuplicates()
    // totalStudentsDropDuplicatesUnionDF.printSchema()
    // totalStudentsDropDuplicatesUnionDF.show(false)
  }

}
