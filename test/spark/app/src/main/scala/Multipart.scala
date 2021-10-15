import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark
import org.apache.log4j.Logger

object Multipart {
  // Part size in a multipart upload.  Must be at least 5 MiB for AWS S3.
  val partSizeBytes = 5 * (1 << 20)

  val logger = Logger.getLogger(getClass.getName)

  def makeCountShards(shards: DataFrame, start: Int, end: Int, numShards: Int): DataFrame = {
    // Generate a BIG dataframe.  Just (tenChars to
    // tenChars+numElements).toDF will blow up allocating everything on the
    // one driver, so generate it in shards.
    implicit val enc: Encoder[Int] = Encoders.scalaInt
    val num = end - start
    def makeShard(n: Int): TraversableOnce[Int] = {
      val s = num.toLong * n / numShards
      val e = math.min(end, num.toLong * (n+1) / numShards)
      (s.toInt to e.toInt)
    }
    shards.flatMap((row) => makeShard(row.getInt(0))).toDF
  }

  def testMultipart(outputPath: String): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MultipartApp")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    sc.hadoopConfiguration.set("fs.s3a.multipartthreshold", logger.toString)

    val tenChars = 1000000000
    val numElements = 3*partSizeBytes
    val numShards = 10000
    val numElementsPerShard = numElements / numShards

    val start = tenChars
    var end = tenChars + numElements - 1
    val shards = (start to end by numShards).toDF
    val df = makeCountShards(shards, start, end, numShards)

    df.repartition(3).write.format("csv").save(outputPath)

    // read the data we just wrote
    logger.info(s"Read counts - path:$outputPath")
    val readDF = spark.read.format("csv").load(outputPath)

    val inBoth = df.intersect(readDF).count()

    if (inBoth != numElements) {
      val numRead = readDF.count()
      logger.error(s"Wrote ${numElements} numbers, read ${numRead}, shared ${inBoth}")
      println(s"Wrote ${numElements} numbers, read ${numRead}, shared ${inBoth}")
      System.exit(1)
    }
  }
}
