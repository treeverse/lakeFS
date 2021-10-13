import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object Multipart {
  // Part size in a multipart upload.  Must be at least 5 MiB for AWS S3.
  val partSizeBytes = 5 * (1 << 20)

  val logger = Logger.getLogger(getClass.getName)

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
    val count = tenChars to tenChars + numElements - 1 // Each such int is >= 11 chars in CSV
    val df = count.toDF

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
