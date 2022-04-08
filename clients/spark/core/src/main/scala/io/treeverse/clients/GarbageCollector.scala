package io.treeverse.clients

import com.google.protobuf.timestamp.Timestamp
import io.treeverse.clients.LakeFSContext.{
  LAKEFS_CONF_API_ACCESS_KEY_KEY,
  LAKEFS_CONF_API_SECRET_KEY_KEY,
  LAKEFS_CONF_API_URL_KEY
}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, _}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{Delete, DeleteObjectsRequest, ObjectIdentifier}

import java.net.URI
import collection.JavaConverters._

object GarbageCollector {

  case class APIConfigurations(apiURL: String, accessKey: String, secretKey: String)

  def getCommitsDF(runID: String, commitDFLocation: String, spark: SparkSession): Dataset[Row] = {
    spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(commitDFLocation)
  }

  private def getRangeTuples(
      commitID: String,
      repo: String,
      conf: APIConfigurations
  ): Set[(String, Array[Byte], Array[Byte])] = {
    val location =
      new ApiClient(conf.apiURL, conf.accessKey, conf.secretKey).getMetaRangeURL(repo, commitID)
    // continue on empty location, empty location is a result of a commit with no metaRangeID (e.g 'Repository created' commit)
    if (location == "") Set()
    else
      SSTableReader
        .forMetaRange(new Configuration(), location)
        .newIterator()
        .map(range =>
          (new String(range.id), range.message.minKey.toByteArray, range.message.maxKey.toByteArray)
        )
        .toSet
  }

  def getRangesDFFromCommits(
      commits: Dataset[Row],
      repo: String,
      conf: APIConfigurations
  ): Dataset[Row] = {
    val get_range_tuples = udf((commitID: String) => {
      getRangeTuples(commitID, repo, conf).toSeq
    })

    commits.distinct
      .select(col("expired"), explode(get_range_tuples(col("commit_id"))).as("range_data"))
      .select(
        col("expired"),
        col("range_data._1").as("range_id"),
        col("range_data._2").as("min_key"),
        col("range_data._3").as("max_key")
      )
      .distinct
  }

  def getRangeAddresses(
      rangeID: String,
      conf: APIConfigurations,
      repo: String
  ): Seq[String] = {
    val location =
      new ApiClient(conf.apiURL, conf.accessKey, conf.secretKey).getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(new Configuration(), location)
      .newIterator()
      .map(a => a.message.address)
      .toSeq
  }

  def getEntryTuples(
      rangeID: String,
      conf: APIConfigurations,
      repo: String
  ): Set[(String, String, Boolean, Long)] = {
    def getSeconds(ts: Option[Timestamp]): Long = {
      ts.getOrElse(0).asInstanceOf[Timestamp].seconds
    }

    val location =
      new ApiClient(conf.apiURL, conf.accessKey, conf.secretKey).getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(new Configuration(), location)
      .newIterator()
      .map(a =>
        (
          new String(a.key),
          new String(a.message.address),
          a.message.addressType.isRelative,
          getSeconds(a.message.lastModified)
        )
      )
      .toSet
  }

  /** @param leftRangeIDs
   *  @param rightRangeIDs
   *  @param conf
   *  @return tuples of type (key, address, isRelative, lastModified) for every address existing in leftRanges and not in rightRanges
   */
  def leftAntiJoinAddresses(
      leftRangeIDs: Set[String],
      rightRangeIDs: Set[String],
      conf: APIConfigurations,
      repo: String
  ): Set[(String, String, Boolean, Long)] = {
    distinctEntryTuples(leftRangeIDs, conf, repo)

    val leftTuples = distinctEntryTuples(leftRangeIDs, conf, repo)
    val rightTuples = distinctEntryTuples(rightRangeIDs, conf, repo)
    leftTuples -- rightTuples
  }

  private def distinctEntryTuples(rangeIDs: Set[String], conf: APIConfigurations, repo: String) = {
    val tuples = rangeIDs.map((rangeID: String) => getEntryTuples(rangeID, conf, repo))
    if (tuples.isEmpty) Set[(String, String, Boolean, Long)]() else tuples.reduce(_.union(_))
  }

  /** receives a dataframe containing active and expired ranges and returns entries contained only in expired ranges
   *
   *  @param ranges dataframe of type   rangeID:String | expired: Boolean
   *  @return dataframe of type  key:String | address:String | relative:Boolean | last_modified:Long
   */
  def getExpiredEntriesFromRanges(
      ranges: Dataset[Row],
      conf: APIConfigurations,
      repo: String
  ): Dataset[Row] = {
    val left_anti_join_addresses = udf((x: Seq[String], y: Seq[String]) => {
      leftAntiJoinAddresses(x.toSet, y.toSet, conf, repo).toSeq
    })
    val expiredRangesDF = ranges.where("expired")
    val activeRangesDF = ranges.where("!expired")

    // ranges existing in expired and not in active
    val uniqueExpiredRangesDF = expiredRangesDF.join(
      activeRangesDF,
      expiredRangesDF("range_id") === activeRangesDF("range_id"),
      "leftanti"
    )

    //  intersecting options are __(____[_____)______]__   __(____[____]___)__   ___[___(__)___]___   ___[__(___]___)__
    //  intersecting ranges  maintain  ( <= ] && [ <= )
    val intersecting =
      udf((aMin: Array[Byte], aMax: Array[Byte], bMin: Array[Byte], bMax: Array[Byte]) => {
        val byteArrayOrdering = Ordering.by((_: Array[Byte]).toIterable)
        byteArrayOrdering.compare(aMin, bMax) <= 0 && byteArrayOrdering.compare(bMin, aMax) <= 0
      })

    val minMax = udf((min: String, max: String) => (min, max))

    // for every expired range - get all intersecting active ranges
    //  expired_range | active_ranges  | min-max (of expired_range)
    val joinActiveByRange = uniqueExpiredRangesDF
      .as("u")
      .join(
        activeRangesDF.as("a"),
        intersecting(col("a.min_key"), col("a.max_key"), col("u.min_key"), col("u.max_key")),
        "left"
      )
      .select(
        col("u.range_id").as("unique_range"),
        col("a.range_id").as("active_range"),
        minMax(col("u.min_key"), col("u.max_key")).as("min-max")
      )

    // group unique_ranges and active_ranges by min-max
    // unique_ranges: Set[String] | active_ranges: Set[String] | min-max
    // for each row, unique_ranges contains ranges with exact min-max, active_ranges contain ranges intersecting with min-max
    val groupByMinMax = joinActiveByRange
      .groupBy("min-max")
      .agg(
        collect_set("unique_range").alias("unique_ranges"),
        collect_set("active_range").alias("active_ranges")
      )

    // add a column of all addresses contained only in unique ranges
    // for every Row - [ collection of unique ranges | collection of active ranges] the column would contain a collection of addresses existing only in unique ranges
    val addresses = groupByMinMax.withColumn(
      "addresses",
      left_anti_join_addresses(col("unique_ranges"), col("active_ranges"))
    )

    addresses
      .select(explode(col("addresses")).as("addresses"))
      .select(
        col("addresses._1").as("key"),
        col("addresses._2").as("address"),
        col("addresses._3").as("relative"),
        col("addresses._4").as("last_modified")
      )
  }

  def getExpiredAddresses(
      repo: String,
      runID: String,
      commitDFLocation: String,
      spark: SparkSession,
      conf: APIConfigurations
  ): Dataset[Row] = {
    val commitsDF = getCommitsDF(runID, commitDFLocation, spark)
    val rangesDF = getRangesDFFromCommits(commitsDF, repo, conf)
    val expired = getExpiredEntriesFromRanges(rangesDF, conf, repo)

    val activeRangesDF = rangesDF.where("!expired")
    subtractDeduplications(expired, activeRangesDF, conf, repo, spark)
  }

  private def subtractDeduplications(
      expired: Dataset[Row],
      activeRangesDF: Dataset[Row],
      conf: APIConfigurations,
      repo: String,
      spark: SparkSession
  ): Dataset[Row] = {
    val activeRangesRDD: RDD[String] =
      activeRangesDF.select("range_id").rdd.distinct().map(x => x.getString(0))
    val activeAddresses: RDD[String] = activeRangesRDD
      .flatMap(range => {
        getRangeAddresses(range, conf, repo)
      })
      .distinct()
    val activeAddressesRows: RDD[Row] = activeAddresses.map(x => Row(x))
    val schema = new StructType().add(StructField("address", StringType, true))
    val activeDF = spark.createDataFrame(activeAddressesRows, schema)
    // remove active addresses from delete candidates
    expired.join(
      activeDF,
      expired("address") === activeDF("address"),
      "leftanti"
    )
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()
    if (args.length != 2) {
      Console.err.println(
        "Usage: ... <repo_name> <region>"
      )
      System.exit(1)
    }

    val repo = args(0)
    val region = args(1)
    val previousRunID =
      "" //args(2) // TODO(Guys): get previous runID from arguments or from storage
    val hc = spark.sparkContext.hadoopConfiguration
    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val res = new ApiClient(apiURL, accessKey, secretKey)
      .prepareGarbageCollectionCommits(repo, previousRunID)
    val runID = res.getRunId
    println("apiURL: " + apiURL)

    val gcCommitsLocation = ApiClient.translateS3(new URI(res.getGcCommitsLocation)).toString
    println("gcCommitsLocation: " + gcCommitsLocation)
    val gcAddressesLocation = ApiClient.translateS3(new URI(res.getGcAddressesLocation)).toString
    println("gcAddressesLocation: " + gcAddressesLocation)
    val expiredAddresses = getExpiredAddresses(repo,
                                               runID,
                                               gcCommitsLocation,
                                               spark,
                                               APIConfigurations(apiURL, accessKey, secretKey)
                                              ).withColumn("run_id", lit(runID))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    expiredAddresses.write
      .partitionBy("run_id")
      .mode(SaveMode.Overwrite)
      .parquet(gcAddressesLocation)

    println("Expired addresses:")
    expiredAddresses.show()

    S3BulkDeleter.remove(repo, gcAddressesLocation, runID, region, spark)
  }
}

object S3BulkDeleter {
  private def repartitionBySize(df: DataFrame, maxSize: Int, column: String): DataFrame = {
    val nRows = df.count()
    val nPartitions = math.max(1, math.floor(nRows / maxSize)).toInt
    df.repartitionByRange(nPartitions, col(column))
  }

  private def delObjIteration(
      bucket: String,
      keys: Seq[String],
      s3Client: S3Client,
      snPrefix: String
  ): Seq[String] = {
    if (keys.isEmpty) None
    val removeKeys =
      keys.map(x => ObjectIdentifier.builder().key(snPrefix.concat(x)).build()).asJava
    val delObj = Delete.builder().objects(removeKeys).build()
    val delObjReq = DeleteObjectsRequest.builder.delete(delObj).bucket(bucket).build()
    val res = s3Client.deleteObjects(delObjReq)
    res.deleted().asScala.map(_.key())
  }

  private def getS3Client(region: String, numRetries: Int) = {
    val retryPolicy = RetryPolicy
      .builder()
      .backoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
      .numRetries(numRetries)
      .build()
    val configuration = ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build()
    S3Client.builder.region(Region.of(region)).overrideConfiguration(configuration).build
  }

  def bulkRemove(
      readKeysDF: DataFrame,
      bulkSize: Int,
      spark: SparkSession,
      bucket: String,
      region: String,
      numRetries: Int,
      snPrefix: String
  ): Dataset[String] = {
    import spark.implicits._
    val repartitionedKeys = repartitionBySize(readKeysDF, bulkSize, "address")
    val bulkedKeyStrings = repartitionedKeys
      .select("address")
      .map(_.getString(0)) // get address as string (address is in index 0 of row)
    bulkedKeyStrings
      .mapPartitions(iter => {
        iter
          .grouped(bulkSize)
          .flatMap(delObjIteration(bucket, _, getS3Client(region, numRetries), snPrefix))
      })
  }

  def remove(
      repo: String,
      addressDFLocation: String,
      runID: String,
      region: String,
      spark: SparkSession
  ) = {
    val MaxBulkSize = 1000
    val awsRetries = 1000

    val hc = spark.sparkContext.hadoopConfiguration
    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val storageNamespace = new ApiClient(apiURL, accessKey, secretKey).getStorageNamespace(repo)
    println("storageNamespace: " + storageNamespace)
    val uri = new URI(storageNamespace)
    val bucket = uri.getHost
    val key = uri.getPath
    val addSuffixSlash = if (key.endsWith("/")) key else key.concat("/")
    val snPrefix =
      if (addSuffixSlash.startsWith("/")) addSuffixSlash.substring(1) else addSuffixSlash
    println("addressDFLocation: " + addressDFLocation)

    val df = spark.read
      .parquet(addressDFLocation)
      .where(col("run_id") === runID)
      .where(col("relative") === true)
    val res =
      bulkRemove(df, MaxBulkSize, spark, bucket, region, awsRetries, snPrefix)
        .toDF("addresses")
        .collect()
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      Console.err.println(
        "Usage: ... <repo_name> <runID> <region> s3://storageNamespace/prepared_addresses_table"
      )
      System.exit(1)
    }
    val repo = args(0)
    val runID = args(1)
    val region = args(2)
    val addressesDFLocation = args(3)
    val spark = SparkSession.builder().getOrCreate()
    remove(repo, addressesDFLocation, runID, region, spark)
  }
}
