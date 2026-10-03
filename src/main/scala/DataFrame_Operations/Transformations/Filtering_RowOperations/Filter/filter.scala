package DataFrame_Operations.Transformations.Filtering_RowOperations.Filter

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// Filter usage in Spark Data frames
object filter {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Filter usage in Spark Data frames")
      .getOrCreate()

    val cols = Seq("Name", "Branch", "Marks")
    val data = Seq(
      ("Anusha", "ECE", 90),
      ("Bindhu", "ECE", 80),
      ("Chitra", "ECE", 70),
      ("Divya", "ECE", 60),
      ("Eesha", "CSE", 90),
      ("Fathima", "CSE", 65),
      ("Ganga", "CSE", 50)
    )

    val df = spark.createDataFrame(data).toDF(cols:_*)
    // df.printSchema()
    // df.show(false)

    // Type 1 :
    // for string type
    val df1 = df.filter("Branch == 'ECE'")
    // df1.printSchema()
    // df1.show(false)
    val df2 = df.filter("Branch = 'CSE'")
    // df2.printSchema()
    // df2.show(false)

    // for integer type
    val df3 = df.filter("Marks == '90'")
    // df3.show(false)
    val df4 = df.filter("Marks = '80'")
    // df4.show(false)
    val df5 = df.filter("Marks == 70")
    // df5.show(false)
    val df6 = df.filter("Marks = 60")
    // df6.show(false)
    val df7 = df.filter("Marks <= '60'")
    // df7.show(false)


    // Type 2 : using col
    val df8 = df.filter(col("Branch") === "CSE")
    // df8.show(false)

    val df9 = df.filter(col("Marks") === 90)
    // df9.show(false)

    val df10 = df.filter(col("Marks") >= 80)
    // df10.show(false)


    // Type 3 :
    val df11 = df.filter(df("Branch") === "CSE")
    // df11.show(false)

    val df12 = df.filter(df("Marks") === 90)
    // df12.show(false)

    val df13 = df.filter(df("Marks") >= 80)
    // df13.show(false)


    // Type 4 : using $
    // for this type we need to import spark implicits
    import spark.implicits._
    // Here it enables the $"column_name" syntax
    val df14 = df.filter($"Name" === "Anusha")
    // df14.show(false)
    val df15 = df.filter($"Marks" > 70)
    // df15.show(false)


    // Multiple conditions (And, OR, NOT)
    // AND (&&) condition
    val df16 = df.filter(col("Branch") === "ECE" && col("Marks") >= 75)
    // df16.show(false)

    val df17 = df.filter(df("Branch") === "CSE" && df("Marks") <= 70)
    // df17.show(false)

    val df18 = df.filter(col("Branch") === "ECE" && df("Marks") === 80)
    // df18.show(false)

    // Chained filters (acts as implicit AND)
    val df19 = df.filter(df("Branch") === "ECE").filter(col("Marks") >= 80)
    // df19.show(false)

    // OR (||) condition
    val df20 = df.filter(df("Branch") === "ECE" || col("Branch") === "CSE")
    // df20.show(false)

    val df21 = df.filter((col("Branch") === "ECE") || (df("Marks") >= 70))
    // df21.show(false)

    // Not condition (!==)
    val df22 = df.filter((col("Branch") !== "ECE") || (df("Marks") >= 70))
    // df22.show(false)

    df.createOrReplaceTempView("Students")
    val studentsDF = spark.sql("select * from Students " +
      "where Branch = 'ECE' and Marks >= 80")
    // studentsDF.show(false)


    // creating a df with null values in it
    val cols23 = Seq("Name", "Marks")
    val data23 : Seq[(String, java.lang.Integer)] = Seq(
      ("Anusha", 90),
      (null, 80),
      ("Chitra", null),
      (null, null)
    )
    val df23 = spark.createDataFrame(data23).toDF(cols23: _*)
    // df23.printSchema()
    // df23.show(false)


    // Null Checking : Find rows where data is missing
    val df24 = df23.filter(col("Name").isNull)
    // df24.printSchema()
    // df24.show(false)

    val df25 = df23.filter(df23("Name").isNull)
    // df25.show(false)

    val df26 = df23.filter($"Name".isNull)
    // df26.show(false)


    // Not Null Checking : Find rows where data is present
    val df27 = df23.filter(col("Name").isNotNull)
    // df27.printSchema()
    // df27.show(false)

    val df28 = df23.filter(df23("Name").isNotNull)
    // df28.show(false)

    val df29 = df23.filter($"Name".isNotNull)
    // df29.show(false)


    // List Membership : Checking the column values present which are integer types
    val df30 = df.filter(col("Marks").isin(40,50,60,70))
    // df30.show(false)

    val df31 = df.filter(df("Marks").isin(40,50,60,70))
    // df31.show(false)

    val df32 = df.filter($"Marks".isin(40,50,60,70))
    // df32.show(false)


    // List Membership : Checking the column values present which are string types
    val df33 = df.filter(col("Branch").isin("ECE","Civil","Mech"))
    // df33.show(false)

    val df34 = df.filter(df("Branch").isin("ECE", "Civil", "Mech"))
    // df34.show(false)

    val df35 = df.filter($"Branch".isin("ECE", "Civil", "Mech"))
    // df35.show(false)


    // String Matching : like
    val df36 = df.filter(col("Name").like("A%"))
    // df36.show(false)

    val df37 = df.filter(df("Name").like("%"))
    // df37.show(false)

    val df38 = df.filter($"Name".like("_n%"))
    // df38.show(false)
    // more examples : "%a%", "__%"


    // String Matching : startsWith
    val df39 = df.filter(col("Name").startsWith("An"))
    // df39.show(false)

    val df40 = df.filter(col("Name").startsWith("Bin"))
    // df40.show(false)

    val df41 = df.filter(col("Name").startsWith("Fa"))
    // df41.show(false)

    // String Matching : contains
    val df42 = df.filter(col("Name").contains("sh"))
    // df42.show(false)

    val df43 = df.filter(col("Name").contains("th"))
    // df43.show(false)

    val df44 = df.filter(col("Name").contains("a"))
    // df44.show(false)

  }
}
