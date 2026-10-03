package DataFrame_Operations.ViewingData.Show

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

// Difference among df.show() , df.show(true) , df.show(false)
object show {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Collect action usage").getOrCreate()

    // from input data1
    val cols = Seq("Name","Marks")
    val data = Seq(
      ("Anusha",90),
      ("Anusha",90),
      ("Bindhu",85),
      ("Chitra",80),
      ("Divya",70)
    )

    val df = spark.createDataFrame(data).toDF(cols:_*)

    // see the difference between them in output df
   /* df.show()
    df.show(true)
    df.show(false) */


    // from input data2
    val cols1 = Seq("Name", "Occupation")
    val data1 = Seq(
      ("Anusha", "Teacher"),
      ("Bindhu", "IT Software Employee"),
      ("Chitra", "She studied MBBS and she is a Doctor")
    )

    val df1 = spark.createDataFrame(data1).toDF(cols1: _*)

    // observe the difference for below 3 in output df
   /* df1.show()
    df1.show(true)
    df1.show(false) */


    // from a csv file having 26 rows
    val df2 = spark.read.option("header","true").option("inferSchema","true")
      .csv("C:\\Spark_Sample_Files\\Show\\showFile.csv")
    // df2.printSchema()

    // All show() methods
   /* 1. show()
    2. show(numRows: Int)
    3. show(truncate: Boolean)
    4. show(numRows: Int, truncate: Boolean)
    5. show(numRows: Int, truncate: Int)
    6. show(numRows: Int, truncate: Int, vertical: Boolean)     // manual addition */

    // 1. show()
    // df2.show()      // This will display first 20 rows and each column is truncated to 20 characters only

    // 2.show(numRows:Int)
    // df2.show(10)           // 10 rows, truncated to 20 characters
    // df2.show(50)          // 50 rows if exist or full rows, truncated to 20 characters

    // 3. show(truncate:Boolean)
    // df2.show(true)             // 20 rows, truncated to 20 characters
    // df2.show(false)            // 20 rows, no truncation

    // 4. show(numRows:Int, truncate:Boolean)
    // df2.show(10,true)        // 10 rows, truncated to 20 characters
    // df2.show(5,false)        // 5 rows, no truncation

    // 5. show(numRows:Int, truncate:Int)
    // df2.show(10,15)    // 10 rows, truncated to 15 characters

    // 6. show(numRows:Int, truncate:int, vertical:Boolean)
    // df2.show(8,12,true)    // 8 rows, truncated to 12 characters, each row displayed vertically
    // df2.show(8,200,false)    // 8 rows, truncated to 200 characters, each row displayed horizontally

    // 7. show(numRows:Int, truncate:int, vertical:Boolean)
    // manually adding truncate and vertical types
    // df2.show(10,truncate=15,vertical=true)    // 10 rows, truncated to 15 characters, each row displayed vertically
    // df2.show(10,truncate=15,vertical=false)    // 10 rows, truncated to 15 characters, each row displayed horizontally

    // Note : df.show() means by default it will set to df.show(numRows:Int=20, truncate:Boolean=true, vertical:Boolean=false)


    // Scenario :
    // For example, If I know my csv file is having 1000 rows then I will do df.show(1000, false) for complete data with no truncation
    // But if my CSV file is having n rows and I don't know the count but I want full all rows to be displayed then
    // df2.show(df2.count().toInt, false)     // here df2 is having 26 rows, so it will display all the rows with no truncation
    // df2.show(Int.MaxValue, false)     // this is another method and give same result as above

  }
}

