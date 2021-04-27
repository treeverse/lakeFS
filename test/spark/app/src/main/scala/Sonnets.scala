import org.apache.spark.sql.SparkSession

object Sonnets {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SonnetsApp")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.docker.lakefs.io:8000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
    var sc = spark.sparkContext
    sc.setLogLevel("DEBUG")
    val sonnets = sc.textFile("s3a://example/master/sonnets.txt")
    val counts = sonnets.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("s3a://example/master/sonnets-wordcount")
    spark.stop()
  }
}
