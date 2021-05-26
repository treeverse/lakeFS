import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object Sonnets {
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SonnetsApp")
      .getOrCreate()
    val input = args.applyOrElse(0, (_: Any) => "s3a://example/main/sonnets.txt")
    val output = args.applyOrElse(1, (_: Any) => "s3a://example/main/sonnets-wordcount")
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    import spark.implicits._
    val sonnets = sc.textFile(input)
    val partitions = 7
    val counts = sonnets
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .repartition(partitions)
      .toDF

    for (fmt <- Seq("csv", "parquet", "json", "orc")) {
      // save data is selected format
      val outputPath = "%s.%s".format(output, fmt)
      logger.info(s"Write word count - format:$fmt path:$outputPath")
      counts.write.format(fmt).save(outputPath)

      // read the data we just wrote
      logger.info(s"Read word count - format:$fmt path:$outputPath")
      val wc = spark.read.format(fmt).load(outputPath)

      // filter word count for specific 'times' and match result
      val expected = "can,or"
      val times = "42"
      val life = wc
        .filter(x => x(1).toString == times) // different formats use different types - compare as string
        .map(x => x(0).toString)
        .collect
        .sorted
        .mkString(",")
      if (life != expected) {
        logger.error(s"Word count - format:$fmt times:$times matched:'$life' expected:'$expected'")
        println(s"Words found $times times, '$life',  doesn't match '$expected' (format:$fmt)")
        System.exit(1)
      }
    }
  }
}
