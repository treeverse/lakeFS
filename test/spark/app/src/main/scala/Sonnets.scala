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
    sc.setLogLevel("INFO")
    import spark.implicits._
    val sonnets = sc.textFile("s3a://example/main/sonnets.txt")
    val counts = sonnets.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).toDF
    counts.write.format("csv").save("s3a://example/main/sonnets-wordcount.csv")

    val wc = spark.read.format("csv").load("s3a://example/main/sonnets-wordcount.csv")
    val life = wc.filter(x => x(1) == "42").map(x => x(0).toString).collect.sorted.mkString(",")
    if (life != "can,or") {
      println(s"Words found 42 times doesn't match '$life'")
      System.exit(1)
    }
  }
}
