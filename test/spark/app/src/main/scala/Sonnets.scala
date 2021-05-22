import org.apache.spark.sql.SparkSession

object Sonnets {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SonnetsApp")
      .getOrCreate()
    val input = args.applyOrElse(0, (_: Any) => "s3a://example/main/sonnets.txt")
    val output = args.applyOrElse(1, (_: Any) => "s3a://example/main/sonnets-wordcount.csv")
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    import spark.implicits._
    val sonnets = sc.textFile(input)
    val counts = sonnets.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).toDF
    counts.write.format("csv").save(output)

    val wc = spark.read.format("csv").load(output)
    val life = wc.filter(x => x(1) == "42").map(x => x(0).toString).collect.sorted.mkString(",")
    if (life != "can,or") {
      println(s"Words found 42 times doesn't match '$life'")
      System.exit(1)
    }
  }
}
