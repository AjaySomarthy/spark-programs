/*

// Reading and writing CSV, Parquet, Avro, ORC, Text, JSON, XML file formats to Spark data frames

1) CSV file format :
   // Writing a data frame to csv file
   df.write.option("header","true").option("InferSchema","true")
       .csv("C:\\FileFormats\\CSV\\students.csv")

   // Reading a csv file to spark data frame
   val studentsCSVDF = spark.read.option("header","true").option("InferSchema","true")
       .csv("C:\\FileFormats\\CSV\\students.csv")

2) Parquet file format :
   // Writing a data frame to parquet file
   df.write.parquet("C:\\FileFormats\\Parquet\\students.parquet")

   // Reading a parquet file to spark data frame
   val studentsParquetDF = spark.read.parquet("C:\\FileFormats\\Parquet\\students.parquet")

3) Avro file format :

   Add this maven dependency to pom.xml file :

   <dependency>
            <groupId>com.databricks</groupId>
            <artifactId>spark-avro_2.11</artifactId>
            <version>4.0.0</version>
        </dependency>

   Then only Avro read and write operations will work.

   // Writing a data frame to avro file
   df.write.format("com.databricks.spark.avro").save("C:\\FileFormats\\Avro\\students.avro")

   // Reading an avro file to spark data frame
   val studentsAvroDF = spark.read.format("com.databricks.spark.avro").load("C:\\FileFormats\\Avro\\students.avro")

4) ORC file format :

   add this maven dependency to pom.xml file :

   <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.11</artifactId>
            <version>2.3.1</version> <!-- Match your Spark version -->
            <!-- Remove <scope>provided</scope> if running locally from your IDE -->
        </dependency>

   then enable hive support with the below :

   val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("Reading and writing all file formats")
      .enableHiveSupport()
      .getOrCreate()

   // Writing a data frame to orc file
   df.write.format("orc").save("C:\\FileFormats\\ORC\\students.orc")

   // Reading an orc file to spark data frame
   val studentsORCDF = spark.read.format("orc").load("C:\\FileFormats\\ORC\\students.orc")

5) Text file format :
   // Writing a data frame to text file
   df.write.option("header", "true").option("inferSchema", "true").option("delimiter", ",")
          .csv("C:\\FileFormats\\Text\\students.txt")

   // reading a text file to spark data frame
   val studentsTextDF = spark.read.option("header", "true").option("inferSchema", "true")
           .option("delimiter", ",").csv("C:\\FileFormats\\Text\\students.txt")

6) JSON file format
   // writing a data frame to a json file
   // df.write.json("C:\\FileFormats\\JSON\\students.json")

   // reading a json file to spark data frame
   val studentsJSONDF = spark.read.json("C:\\FileFormats\\JSON\\students.json")

7) XML file format

   add this maven dependency to pom.xml file :

   <dependency>
            <groupId>com.databricks</groupId>
            <artifactId>spark-xml_2.11</artifactId>
            <version>0.6.0</version>
        </dependency>

   Then only xml read and write operations will work.

   // writing a data frame to a xml file
   df.write.format("com.databricks.spark.xml").option("rootTag","people").option("rowTag","ppl")
           .save("C:\\FileFormats\\XML\\students.xml")

   // reading a xml file to spark data frame
   val studentsXMLDF = spark.read.format("com.databricks.spark.xml").option("rowTag", "ppl")
            .load("C:\\FileFormats\\XML\\students.xml")


 */