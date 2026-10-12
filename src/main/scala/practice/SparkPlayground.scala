package practice

object SparkPlayground {

  def main(args:Array[String]):Unit={

    println("Welcome to Spark Scala programs")

    println(
      "Hadoop Version: " +
        org.apache.hadoop.util.VersionInfo.getVersion
    )

  }

}
