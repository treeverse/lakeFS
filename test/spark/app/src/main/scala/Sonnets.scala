import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

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
    sc.setLogLevel("TRACE")
    import spark.implicits._
    val sonnets = sc.textFile(input)
    val partitions = 7
    val counts = sonnets
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map({ case (word, count) => (word, count, if (word != "") word.substring(0, 1) else "") })
      .toDF("word", "count", "partition_key")
      .repartition(partitions, $"partition_key")

    var failed = new ListBuffer[String]

    for (fmt <- Seq("csv", "parquet", "json", "orc")) {
      try {
        // save data is selected format
        val outputPath = s"${output}.${fmt}"
        logger.info(s"Write word count - format:$fmt path:$outputPath")
        val targetBase = counts.write.partitionBy("partition_key").mode(SaveMode.Overwrite).format(fmt)
        val target = (
          if (fmt == "csv")
            targetBase.option("inferSchema", "true").option("header", "true")
          else targetBase
        )
        target.save(outputPath)

        // read the data we just wrote
        logger.info(s"Read word count - format:$fmt path:$outputPath")
        val sourceBase = spark.read.format(fmt)
        val source = (
          if (fmt == "csv")
            sourceBase.option("inferSchema", "true").option("header", "true")
          else sourceBase
        )
        val data = source.load(outputPath)

        // filter word count for specific 'times' and match result
        val expected = "can,or"
        val times = "42"
        val life = data
          .filter($"count".cast("string") === times) // different formats use different types - compare as string
          .map(_.getAs[String]("word"))
          .collect
          .sorted
          .mkString(",")
        if (life != expected) {
          logger.error(s"Word count - format:$fmt times:$times matched:'$life' expected:'$expected'")
          println(s"Words found $times times, '$life',  doesn't match '$expected' (format:$fmt)")
          failed = failed :+ fmt
        }
      } catch {
        case e: (Exception) =>
          logger.error(s"Format $fmt unexpected exception: ${e}")
          failed = failed :+ fmt
      }
    }

    if (!failed.isEmpty) {
      logger.error(s"FAIL  formats: ${failed}\n")
      System.exit(1)
    }
  }
}
