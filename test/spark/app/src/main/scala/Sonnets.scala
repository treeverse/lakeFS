import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.fs.{Path, RemoteIterator}
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object Sonnets {
  // Make RemoteIterator a Scala iterator.  Reformatted version of
  // https://gist.github.com/timvw/4ec727de9b76d9afc51298d9e68c4241.
  /**
   * Converts RemoteIterator from Hadoop to Scala Iterator that provides all the familiar functions such as map,
   * filter, foreach, etc.
   *
   * @param underlying The RemoteIterator that needs to be wrapped
   * @tparam T Items inside the iterator
   * @return Standard Scala Iterator
   */
  def convertToScalaIterator[T](underlying: RemoteIterator[T]): Iterator[T] = {
    case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
      override def hasNext = underlying.hasNext

      override def next = underlying.next
    }
    wrapper(underlying)
  }

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

        {
          /*
           *  Verify all files match one of:
           *  - s"${outputPath}/partition_key=PPP/XXXYYYZZZ.${fmt}"
           * -  s"${outputPath}/_SUCCESS"
           */
          val pattern: Regex = s"${Regex.quote(outputPath)}/(partition_key=[^/]*/[^/]*\\.${Regex.quote(fmt)}|_SUCCESS)".r
          val path = new Path(outputPath)
          val fs = path.getFileSystem(sc.hadoopConfiguration)
          val badFiles = convertToScalaIterator(fs.listFiles(path, true)).
            map(file => file.getPath.toString).
            filter(name => !(name matches pattern.toString))
          if (!badFiles.isEmpty) {
            logger.error(s"Unexpected leftover files generating ${outputPath}: ${badFiles.mkString(", ")}")
            failed = failed :+ fmt
          }
        }

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
