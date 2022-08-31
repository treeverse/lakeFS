package io.treeverse.clients

import com.google.protobuf.timestamp.Timestamp
import io.treeverse.clients.LakeFSContext.{
  LAKEFS_CONF_API_ACCESS_KEY_KEY,
  LAKEFS_CONF_API_SECRET_KEY_KEY,
  LAKEFS_CONF_API_URL_KEY,
  LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY,
  LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY
}
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, _}
import com.amazonaws.services.s3.AmazonS3

import java.net.URI
import collection.JavaConverters._
import java.time.format.DateTimeFormatter
import org.json4s.native.JsonMethods._
import org.json4s._
import org.json4s.JsonDSL._

/** Interface to build an S3 client.  The object
 *  io.treeverse.clients.conditional.S3ClientBuilder -- conditionally
 *  defined in a separate file according to the supported Hadoop version --
 *  implements this trait.  (Scala requires companion objects to be defined
 *  in the same file, so it cannot be a companion.)
 */
trait S3ClientBuilder extends Serializable {

  /** Return a configured Amazon S3 client similar to the one S3A would use.
   *  On Hadoop versions >=3, S3A can assume a role, and the returned S3
   *  client will similarly assume that role.
   *
   *  @param hc         (partial) Hadoop configuration of fs.s3a.
   *  @param bucket     that this client will access.
   *  @param region     to find this bucket.
   *  @param numRetries number of times to retry on AWS.
   */
  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3
}

object GarbageCollector {

  type ConfMap = List[(String, String)]

  /** @return a serializable summary of values in hc starting with prefix.
   */
  def getHadoopConfigurationValues(hc: Configuration, prefix: String): ConfMap =
    hc.iterator.asScala
      .filter(_.getKey.startsWith(prefix))
      .map(entry => (entry.getKey, entry.getValue))
      .toList
      .asInstanceOf[ConfMap]

  def configurationFromValues(v: Broadcast[ConfMap]) = {
    val hc = new Configuration()
    v.value.foreach({ case (k, v) => hc.set(k, v) })
    hc
  }

  def getCommitsDF(runID: String, commitDFLocation: String, spark: SparkSession): Dataset[Row] = {
    spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(commitDFLocation)
  }

  private def getRangeTuples(
      commitID: String,
      repo: String,
      apiConf: APIConfigurations,
      hcValues: Broadcast[ConfMap]
  ): Set[(String, Array[Byte], Array[Byte])] = {
    val location = ApiClient
      .get(apiConf)
      .getMetaRangeURL(repo, commitID)
    // continue on empty location, empty location is a result of a commit with no metaRangeID (e.g 'Repository created' commit)
    if (location == "") Set()
    else
      SSTableReader
        .forMetaRange(configurationFromValues(hcValues), location)
        .newIterator()
        .map(range =>
          (new String(range.id), range.message.minKey.toByteArray, range.message.maxKey.toByteArray)
        )
        .toSet
  }

  def getRangesDFFromCommits(
      commits: Dataset[Row],
      repo: String,
      apiConf: APIConfigurations,
      hcValues: Broadcast[ConfMap]
  ): Dataset[Row] = {
    val get_range_tuples = udf((commitID: String) => {
      getRangeTuples(commitID, repo, apiConf, hcValues).toSeq
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
      apiConf: APIConfigurations,
      repo: String,
      hcValues: Broadcast[ConfMap]
  ): Seq[String] = {
    val location = ApiClient
      .get(apiConf)
      .getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(configurationFromValues(hcValues), location)
      .newIterator()
      .map(a => a.message.address)
      .toSeq
  }

  def getEntryTuples(
      rangeID: String,
      apiConf: APIConfigurations,
      repo: String,
      hcValues: Broadcast[ConfMap]
  ): Set[(String, String, Boolean, Long)] = {
    def getSeconds(ts: Option[Timestamp]): Long = {
      ts.getOrElse(0).asInstanceOf[Timestamp].seconds
    }

    val location = ApiClient
      .get(apiConf)
      .getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(configurationFromValues(hcValues), location)
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
   *  @param apiConf
   *  @return tuples of type (key, address, isRelative, lastModified) for every address existing in leftRanges and not in rightRanges
   */
  def leftAntiJoinAddresses(
      leftRangeIDs: Set[String],
      rightRangeIDs: Set[String],
      apiConf: APIConfigurations,
      repo: String,
      hcValues: Broadcast[ConfMap]
  ): Set[(String, String, Boolean, Long)] = {
    distinctEntryTuples(leftRangeIDs, apiConf, repo, hcValues)

    val leftTuples = distinctEntryTuples(leftRangeIDs, apiConf, repo, hcValues)
    val rightTuples = distinctEntryTuples(rightRangeIDs, apiConf, repo, hcValues)
    leftTuples -- rightTuples
  }

  private def distinctEntryTuples(
      rangeIDs: Set[String],
      apiConf: APIConfigurations,
      repo: String,
      hcValues: Broadcast[ConfMap]
  ) = {
    val tuples =
      rangeIDs.map((rangeID: String) => getEntryTuples(rangeID, apiConf, repo, hcValues))
    if (tuples.isEmpty) Set[(String, String, Boolean, Long)]() else tuples.reduce(_.union(_))
  }

  /** receives a dataframe containing active and expired ranges and returns entries contained only in expired ranges
   *
   *  @param ranges dataframe of type   rangeID:String | expired: Boolean
   *  @return dataframe of type  key:String | address:String | relative:Boolean | last_modified:Long
   */
  def getExpiredEntriesFromRanges(
      ranges: Dataset[Row],
      apiConf: APIConfigurations,
      repo: String,
      hcValues: Broadcast[ConfMap]
  ): Dataset[Row] = {
    val left_anti_join_addresses = udf((x: Seq[String], y: Seq[String]) => {
      leftAntiJoinAddresses(x.toSet, y.toSet, apiConf, repo, hcValues).toSeq
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
      apiConf: APIConfigurations,
      hcValues: Broadcast[ConfMap]
  ): Dataset[Row] = {
    val commitsDF = getCommitsDF(runID, commitDFLocation, spark)
    val rangesDF = getRangesDFFromCommits(commitsDF, repo, apiConf, hcValues)
    val expired = getExpiredEntriesFromRanges(rangesDF, apiConf, repo, hcValues)

    val activeRangesDF = rangesDF.where("!expired")
    subtractDeduplications(expired, activeRangesDF, apiConf, repo, spark, hcValues)
  }

  private def subtractDeduplications(
      expired: Dataset[Row],
      activeRangesDF: Dataset[Row],
      apiConf: APIConfigurations,
      repo: String,
      spark: SparkSession,
      hcValues: Broadcast[ConfMap]
  ): Dataset[Row] = {
    val activeRangesRDD: RDD[String] =
      activeRangesDF.select("range_id").rdd.distinct().map(x => x.getString(0))
    val activeAddresses: RDD[String] = activeRangesRDD
      .flatMap(range => {
        getRangeAddresses(range, apiConf, repo, hcValues)
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

    val spark = SparkSession.builder().appName("GarbageCollector").getOrCreate()
    val hc = spark.sparkContext.hadoopConfiguration

    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val connectionTimeout = hc.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY)
    val readTimeout = hc.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
    val apiClient = ApiClient.get(
      APIConfigurations(apiURL, accessKey, secretKey, connectionTimeout, readTimeout)
    )
    val storageType = apiClient.getBlockstoreType()

    if (storageType == StorageUtils.StorageTypeS3 && args.length != 2) {
      Console.err.println(
        "Usage: ... <repo_name> <region>"
      )
      System.exit(1)
    } else if (storageType == StorageUtils.StorageTypeAzure && args.length != 1) {
      Console.err.println(
        "Usage: ... <repo_name>"
      )
    }

    val repo = args(0)
    val region = if (args.length == 2) args(1) else null
    val previousRunID =
      "" //args(2) // TODO(Guys): get previous runID from arguments or from storage

    // Spark operators will need to generate configured FileSystems to read
    // ranges and metaranges.  They will not have a JobContext to let them
    // do that.  Transmit (all) Hadoop filesystem configuration values to
    // let them generate a (close-enough) Hadoop configuration to build the
    // needed FileSystems.
    val hcValues = spark.sparkContext.broadcast(getHadoopConfigurationValues(hc, "fs."))

    var gcRules: String = ""
    try {
      gcRules = apiClient.getGarbageCollectionRules(repo)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        println("No GC rules found for repository: " + repo)
        // Exiting with a failure status code because users should not really run gc on repos without GC rules.
        System.exit(2)
    }

    val res = apiClient.prepareGarbageCollectionCommits(repo, previousRunID)
    val runID = res.getRunId
    println("apiURL: " + apiURL)

    val gcCommitsLocation =
      ApiClient.translateURI(new URI(res.getGcCommitsLocation), storageType).toString
    println("gcCommitsLocation: " + gcCommitsLocation)
    val gcAddressesLocation =
      ApiClient.translateURI(new URI(res.getGcAddressesLocation), storageType).toString
    println("gcAddressesLocation: " + gcAddressesLocation)
    val expiredAddresses =
      getExpiredAddresses(
        repo,
        runID,
        gcCommitsLocation,
        spark,
        APIConfigurations(apiURL, accessKey, secretKey, connectionTimeout, readTimeout),
        hcValues
      ).withColumn("run_id", lit(runID))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    expiredAddresses.write
      .partitionBy("run_id")
      .mode(SaveMode.Overwrite)
      .parquet(gcAddressesLocation)

    println("Expired addresses:")
    expiredAddresses.show()

    // The remove operation uses an SDK client to directly access the underlying storage, and therefore does not need
    // a translated storage namespace that triggers processing by Hadoop FileSystems.
    var storageNSForSdkClient = apiClient.getStorageNamespace(repo, StorageClientType.SDKClient)
    if (!storageNSForSdkClient.endsWith("/")) {
      storageNSForSdkClient += "/"
    }

    val removed =
      remove(storageNSForSdkClient,
             gcAddressesLocation,
             expiredAddresses,
             runID,
             region,
             hcValues,
             storageType
            )

    var storageNSForHadoopFS = apiClient.getStorageNamespace(repo, StorageClientType.HadoopFS)
    if (!storageNSForHadoopFS.endsWith("/")) {
      storageNSForHadoopFS += "/"
    }

    val commitsDF = getCommitsDF(runID, gcCommitsLocation, spark)
    val reportLogsDst = concatToGCLogsPrefix(storageNSForHadoopFS, "summary")
    val reportExpiredDst = concatToGCLogsPrefix(storageNSForHadoopFS, "expired_addresses")

    val time = DateTimeFormatter.ISO_INSTANT.format(java.time.Clock.systemUTC.instant())
    writeParquetReport(commitsDF, reportLogsDst, time, "commits.parquet")
    writeParquetReport(expiredAddresses, reportExpiredDst, time)
    writeJsonSummary(reportLogsDst, removed.count(), gcRules, hcValues, time)

    removed
      .withColumn("run_id", lit(runID))
      .write
      .partitionBy("run_id")
      .mode(SaveMode.Overwrite)
      .parquet(
        concatToGCLogsPrefix(storageNSForHadoopFS, s"deleted_objects/$time/deleted.parquet")
      )

    spark.close()
  }

  private def concatToGCLogsPrefix(storageNameSpace: String, key: String): String = {
    val strippedKey = key.stripPrefix("/")
    s"${storageNameSpace}_lakefs/logs/gc/$strippedKey"
  }

  private def repartitionBySize(df: DataFrame, maxSize: Int, column: String): DataFrame = {
    val nRows = df.count()
    val nPartitions = math.max(1, math.ceil(nRows / maxSize)).toInt
    df.repartitionByRange(nPartitions, col(column))
  }

  def bulkRemove(
      readKeysDF: DataFrame,
      storageNamespace: String,
      region: String,
      hcValues: Broadcast[ConfMap],
      storageType: String
  ): Dataset[String] = {
    val bulkRemover =
      BulkRemoverFactory(storageType, configurationFromValues(hcValues), storageNamespace, region)
    val bulkSize = bulkRemover.getMaxBulkSize()
    val spark = org.apache.spark.sql.SparkSession.active
    import spark.implicits._
    val repartitionedKeys = repartitionBySize(readKeysDF, bulkSize, "address")
    val bulkedKeyStrings = repartitionedKeys
      .select("address")
      .map(_.getString(0)) // get address as string (address is in index 0 of row)

    bulkedKeyStrings
      .mapPartitions(iter => {
        // mapPartitions lambda executions are sent over to Spark executors, the executors don't have access to the
        // bulkRemover created above because it was created on the driver and it is not a serializeable object. Therefore,
        // we create new bulkRemovers.
        val bulkRemover = BulkRemoverFactory(storageType,
                                             configurationFromValues(hcValues),
                                             storageNamespace,
                                             region
                                            )
        iter
          .grouped(bulkSize)
          .flatMap(bulkRemover.deleteObjects(_, storageNamespace))
      })
  }

  def remove(
      storageNamespace: String,
      addressDFLocation: String,
      expiredAddresses: Dataset[Row],
      runID: String,
      region: String,
      hcValues: Broadcast[ConfMap],
      storageType: String
  ) = {
    println("addressDFLocation: " + addressDFLocation)

    val df = expiredAddresses
      .where(col("run_id") === runID)
      .where(col("relative") === true)

    bulkRemove(df, storageNamespace, region, hcValues, storageType).toDF("addresses")
  }

  private def writeParquetReport(
      df: DataFrame,
      dstRoot: String,
      time: String,
      suffix: String = ""
  ) = {
    val dstPath = s"${dstRoot}/dt=${time}/${suffix}"
    df.write.parquet(dstPath)
  }

  private def writeJsonSummary(
      dstRoot: String,
      numDeletedObjects: Long,
      gcRules: String,
      hcValues: Broadcast[ConfMap],
      time: String
  ) = {
    val dstPath = new Path(s"${dstRoot}/dt=${time}/summary.json")
    val dstFS = dstPath.getFileSystem(configurationFromValues(hcValues))
    val jsonSummary = JObject("gc_rules" -> gcRules, "num_deleted_objects" -> numDeletedObjects)

    val stream = dstFS.create(dstPath)
    try {
      stream.writeChars(compact(render(jsonSummary)))
    } finally {
      stream.close()
    }
  }
}
