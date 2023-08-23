package org.corerda.service.spark

// Spark Session's Single Point of Access
// convenience wrapper to declare and safely access the session
object Session {
  lazy val spark = manuallyInit
  val implicits = PimpedSpark

  import org.apache.spark.sql.SparkSession
  private def manuallyInit: SparkSession = {
    val ss = SparkSession.builder
      .master("local[*]")
      .appName("Tao-spark")
      .getOrCreate()

    val sc = ss.sparkContext

    // TODO - set/configure the Log4j to display info
    sc.setLogLevel("WARN")
    //  (number nodes x cores per node)
    println(s"\n#### [INFO] Thread Execution: ${sc.defaultParallelism} Partitions in use ####\n")

    ss
  }
}