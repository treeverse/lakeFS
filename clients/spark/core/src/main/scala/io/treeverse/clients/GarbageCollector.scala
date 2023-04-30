package io.treeverse.clients

import com.amazonaws.services.s3.AmazonS3
import io.lakefs.clients.api.model.GarbageCollectionPrepareResponse
import io.treeverse.clients.LakeFSContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.HashPartitioner
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.net.URI
import java.util.UUID
import org.apache.commons.lang3.StringUtils

trait RangeGetter extends Serializable {

  /** @return all rangeIDs in metarange of commitID on repo.
   */
  def getRangeIDs(commitID: String, repo: String): Iterator[String]

  /** @return all object addresses in range rangeID on repo.
   */
  def getRangeEntries(rangeID: String, repo: String): Iterator[io.treeverse.lakefs.catalog.Entry]
}

class LakeFSRangeGetter(val apiConf: APIConfigurations, val configMapper: ConfigMapper)
    extends RangeGetter {
  def getRangeIDs(commitID: String, repo: String): Iterator[String] = {
    val conf = configMapper.configuration
    val apiClient = ApiClient.get(apiConf)
    val commit = apiClient.getCommit(repo, commitID)
    val maxCommitEpochSeconds = conf.getLong(LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_EPOCH_SECONDS_KEY, -1)
    if (maxCommitEpochSeconds > 0 && commit.getCreationDate > maxCommitEpochSeconds) {
      return Iterator.empty
    }
    val location = apiClient.getMetaRangeURL(repo, commit)
    // continue on empty location, empty location is a result of a commit
    // with no metaRangeID (e.g 'Repository created' commit)
    if (location == "") Iterator.empty
    else
      SSTableReader
        .forMetaRange(conf, location)
        .newIterator()
        .map(o => new String(o.id))
  }

  def getRangeEntries(
      rangeID: String,
      repo: String
  ): Iterator[io.treeverse.lakefs.catalog.Entry] = {
    val location = ApiClient
      .get(apiConf)
      .getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(configMapper.configuration, location)
      .newIterator()
      .map(_.message)
  }
}

/** Interface to build an S3 client.  The object
 *  io.treeverse.clients.conditional.S3ClientBuilder -- conditionally
 *  defined in a separate file according to the supported Hadoop version --
 *  implements this trait.  (Scala requires companion objects to be defined
 *  in the same file, so it cannot be a companion.)
 */
trait S3ClientBuilder extends Serializable {

  /** Name of property from which S3A fetches whether to use path-style S3
   *  access or host-style.
   *
   *  org.apache.hadoop.fs.s3a.Constants defines this property only starting
   *  with version 2.8.  Define it here to support earlier versions.
   */
  protected val S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access"

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

class GarbageCollector(val rangeGetter: RangeGetter) extends Serializable {
  @transient lazy val spark = SparkSession.active

  def getCommitsDF(commitDFLocation: String): Dataset[Row] = {
    spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(commitDFLocation)
  }

  def getRangeIDsForCommits(
      commitIDs: Dataset[(String, Boolean)],
      repo: String
  ): Dataset[(String, Boolean)] = {
    import spark.implicits._

    commitIDs.flatMap({ case (commitID, expire) =>
      rangeGetter
        .getRangeIDs(commitID, repo)
        .map((_, expire))
    })
  }

  def getAddressesToDelete(
      rangeIDs: Dataset[(String, Boolean)],
      repo: String,
      storageNS: String,
      numRangePartitions: Int,
      numAddressPartitions: Int,
      approxNumRangesToSpreadPerPartition: Double,
      sampleFraction: Double
  ): Dataset[String] = {
    import spark.implicits._

    // Filter out addresses outside the storage namespace.
    // Returned addresses are either relative ones, or absolute ones that
    // are under the storage namespace.
    def getOwnedAddresses(rangeID: String): Iterator[String] = {
      println(s"getAddressesToDelete: get addresses for range $rangeID")
      val addresses = rangeGetter
        .getRangeEntries(rangeID, repo)
        .filter(e => e.addressType.isRelative || e.address.startsWith(storageNS))
        .map(e =>
          if (e.addressType.isRelative) e.address
          else StringUtils.removeStart(e.address, storageNS)
        )
      addresses
    }

    def getOwnedAddressesUnion(it: Iterator[(String, Boolean)]): Iterator[(String, Boolean)] = {
      val all = collection.mutable.Set[(String, Boolean)]()
      it.foreach({
        case (rangeID, expire) => {
          all ++= getOwnedAddresses(rangeID).map((x) => (x, expire))
        }
      })
      all.iterator
    }

    def readRanges(ranges: Dataset[(String, Boolean)], name: String): RDD[(String, Boolean)] = {
      val approxNumRows = ranges.cache().rdd.countApprox(10000 /* msecs */, 0.9)
      val numPartitions = (try {
        approxNumRows.getFinalValue.high
      } catch {
        case e: Exception => {
          println(s"readRanges: Approximate count for $name failed, count precisely ($e)")
          ranges.count
        }
      }) / approxNumRangesToSpreadPerPartition
      println(s"readRanges: use $numPartitions partitions for $name")

      val all = ranges
        .repartition(Math.max(1, Math.ceil(numPartitions)).toInt)
        .rdd
        .mapPartitionsWithIndex((index: Int, it: Iterator[(String, Boolean)]) => {
          println(s"get addresses partition ${index}")
          getOwnedAddressesUnion(it)
        })
      if (sampleFraction == 1) all
      else {
        println(s"readRanges: downsample to ${sampleFraction}")
        all.sample(true, sampleFraction)
      }
    }

    // Expire an object _only_ if it is always expired!
    val shouldExpire = (expireA: Boolean, expireB: Boolean) => expireA & expireB

    val dedupedRangeIDs = rangeIDs.rdd
      .aggregateByKey(true, numRangePartitions)(shouldExpire, shouldExpire)
      .toDS

    // TODO(ariels): Does this partitioner really apply only to the _keys_?
    val partitioner = new HashPartitioner(numAddressPartitions)
    val addresses = readRanges(dedupedRangeIDs, "ranges")
      .aggregateByKey(true, partitioner)(shouldExpire, shouldExpire)
      .persist(StorageLevel.MEMORY_ONLY_SER)

    // Report some rows on expiry proportions
//    addresses.sample(true, 0.01).toDS.show()

    addresses
      .filter({ case (_, expire) => expire })
      .map({ case (toDelete, _) => toDelete })
      .toDS
  }

  def getExpiredAddresses(
      repo: String,
      storageNS: String,
      commitDFLocation: String,
      numCommitPartitions: Int,
      numRangePartitions: Int,
      numAddressPartitions: Int,
      approxNumRangesToSpreadPerPartition: Double,
      sampleFraction: Double
  ): Dataset[String] = {
    import spark.implicits._

    val commitsDS = getCommitsDF(commitDFLocation)
      .as[(String, Boolean)]
      .repartition(numCommitPartitions)
      .persist(StorageLevel.MEMORY_ONLY_SER)
    val rangeIDs = getRangeIDsForCommits(commitsDS, repo)

    try {
      getAddressesToDelete(
        rangeIDs,
        repo,
        storageNS,
        numRangePartitions,
        numAddressPartitions,
        approxNumRangesToSpreadPerPartition,
        sampleFraction
      )
    } finally {
      commitsDS.unpersist()
    }
  }
}

object GarbageCollector {
  final val GARBAGE_COLLECTOR_SOURCE_NAME = "gc"

  lazy val spark = SparkSession.builder().appName("GarbageCollector").getOrCreate()

  /** @return a serializable summary of values in hc starting with prefix.
   */
  def getHadoopConfigMapper(hc: Configuration, prefixes: String*): ConfigMapper =
    new ConfigMapper(
      spark.sparkContext.broadcast(HadoopUtils.getHadoopConfigurationValues(hc, prefixes: _*))
    )

  private def validateArgsByStorageType(storageType: String, args: Array[String]) = {
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
  }

  /** This function validates that at least one of the mark or sweep flags is true, and that if only sweep is true, then a mark ID is provided.
   *  If 'lakefs.debug.gc.no_delete' is passed or if the above is not true, the function will stop the execution of the GC and exit.
   */
  private def validateRunModeConfigs(
      noDeleteFlag: Boolean,
      shouldMark: Boolean,
      shouldSweep: Boolean,
      markID: String
  ): Unit = {
    if (noDeleteFlag) {
      Console.err.printf("The \"%s\" configuration is deprecated. Use \"%s=false\" instead",
                         LAKEFS_CONF_DEBUG_GC_NO_DELETE_KEY,
                         LAKEFS_CONF_GC_DO_SWEEP
                        )
      System.exit(1)
    }

    if (!shouldMark && !shouldSweep) {
      Console.out.println("Nothing to do, must specify at least one of mark, sweep. Exiting...")
      System.exit(2)
    } else if (!shouldMark && markID.isEmpty) { // Sweep-only mode but no mark ID to sweep
      Console.out.printf("Please provide a mark ID (%s) for sweep-only mode. Exiting...\n",
                         LAKEFS_CONF_GC_MARK_ID
                        )
      System.exit(2)
    }
  }

  def main(args: Array[String]) {
    val hc = spark.sparkContext.hadoopConfiguration

    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val connectionTimeout = hc.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY)
    val readTimeout = hc.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
    val maxCommitIsoDatetime = hc.get(LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_ISO_DATETIME_KEY, "")

    val shouldMark = hc.getBoolean(LAKEFS_CONF_GC_DO_MARK, true)
    val shouldSweep = hc.getBoolean(LAKEFS_CONF_GC_DO_SWEEP, true)

    validateRunModeConfigs(hc.getBoolean(LAKEFS_CONF_DEBUG_GC_NO_DELETE_KEY, false),
                           shouldMark,
                           shouldSweep,
                           hc.get(LAKEFS_CONF_GC_MARK_ID, "")
                          )

    val markID = hc.get(LAKEFS_CONF_GC_MARK_ID, UUID.randomUUID().toString)

    println(s"Got mark_id $markID")

    if (maxCommitIsoDatetime.nonEmpty) {
      hc.setLong(
        LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_EPOCH_SECONDS_KEY,
        LocalDateTime
          .parse(hc.get(LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_ISO_DATETIME_KEY),
                 DateTimeFormatter.ISO_DATE_TIME
                )
          .toEpochSecond(ZoneOffset.UTC)
      )
    }
    val apiConf =
      APIConfigurations(apiURL,
                        accessKey,
                        secretKey,
                        connectionTimeout,
                        readTimeout,
                        GARBAGE_COLLECTOR_SOURCE_NAME
                       )
    val apiClient = ApiClient.get(apiConf)
    val storageType = apiClient.getBlockstoreType()

    validateArgsByStorageType(storageType, args)

    val repo = args(0)

    // TODO(ariels): Allow manual configuration.  Read these rules _only_
    //     for metadata, so users might write something else.
    val gcRules: String =
      try {
        apiClient.getGarbageCollectionRules(repo)
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          println("No GC rules found for repository: " + repo)
          // Exiting with a failure status code because users should not really run gc on repos without GC rules.
          sys.exit(2)
      }
    // Spark operators will need to generate configured FileSystems to read
    // ranges and metaranges.  They will not have a JobContext to let them
    // do that.  Transmit (all) Hadoop filesystem configuration values to
    // let them generate a (close-enough) Hadoop configuration to build the
    // needed FileSystems.
    val hcValues =
      spark.sparkContext.broadcast(HadoopUtils.getHadoopConfigurationValues(hc, "fs.", "lakefs."))
    val configMapper = new ConfigMapper(hcValues)
    val gc = new GarbageCollector(new LakeFSRangeGetter(apiConf, configMapper))
    var storageNSForHadoopFS = apiClient.getStorageNamespace(repo, StorageClientType.HadoopFS)
    if (!storageNSForHadoopFS.endsWith("/")) {
      storageNSForHadoopFS += "/"
    }

    var gcAddressesLocation = ""
    var gcCommitsLocation = ""
    var runID = ""
    var expiredAddresses: DataFrame = null
    val storageNSForSdkClient = getStorageNSForSdkClient(apiClient: ApiClient, repo)

    if (shouldMark) {
      val markInfo =
        markAddresses(gc,
                      apiClient,
                      repo,
                      hc,
                      storageType,
                      apiURL,
                      markID,
                      storageNSForSdkClient,
                      storageNSForHadoopFS
                     )
      gcAddressesLocation = markInfo._1
      gcCommitsLocation = markInfo._2
      expiredAddresses = markInfo._3
      runID = markInfo._4
    }

    val schema = StructType(Array(StructField("addresses", StringType, nullable = false)))
    val removed = {
      if (shouldSweep) {
        // If a mark didn't happen in this run, gcAddressesLocation will be empty and expiredAddresses will be null.
        if (gcAddressesLocation.isEmpty) {
          gcAddressesLocation = getAddressesLocation(storageNSForHadoopFS)
        }
        if (expiredAddresses == null) {
          expiredAddresses = readExpiredAddresses(gcAddressesLocation, markID)
        }

        val region = getRegion(args)

        remove(configMapper,
               storageNSForSdkClient,
               gcAddressesLocation,
               expiredAddresses,
               markID,
               region,
               storageType,
               schema
              )
      } else {
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      }
    }

//    It is necessary to fetch the run ID and commit location if we did not mark in this run (sweep-only mode).
    if (!shouldMark) {
      val runIDAndCommitsLocation = populateRunIDAndCommitsLocation(markID, gcAddressesLocation)
      runID = runIDAndCommitsLocation(0)
      gcCommitsLocation = runIDAndCommitsLocation(1)
    }

    val commitsDF = gc.getCommitsDF(gcCommitsLocation)
    writeReports(storageNSForHadoopFS,
                 gcRules,
                 runID,
                 markID,
                 commitsDF,
                 expiredAddresses,
                 removed,
                 configMapper
                )
    spark.close()
  }

  private def markAddresses(
      gc: GarbageCollector,
      apiClient: ApiClient,
      repo: String,
      hc: Configuration,
      storageType: String,
      apiURL: String,
      markID: String,
      storageNS: String,
      storageNSForHadoopFS: String
  ): (String, String, DataFrame, String) = {
    val runIDToReproduce = hc.get(LAKEFS_CONF_DEBUG_GC_REPRODUCE_RUN_ID_KEY, "")
    val previousRunID = hc.get(LAKEFS_CONF_GC_PREV_RUN_ID, "")

    var prepareResult: GarbageCollectionPrepareResponse = null
    var runID = ""
    var gcCommitsLocation = ""
    var gcAddressesLocation = ""
    if (runIDToReproduce == "") {
      prepareResult = apiClient.prepareGarbageCollectionCommits(repo, previousRunID)
      runID = prepareResult.getRunId
      gcCommitsLocation = hc.get(
        "debug.gc.commits",
        ApiClient.translateURI(new URI(prepareResult.getGcCommitsLocation), storageType).toString
      )
      println("gcCommitsLocation: " + gcCommitsLocation)
      gcAddressesLocation = hc.get(
        "debug.gc.addresses",
        ApiClient.translateURI(new URI(prepareResult.getGcAddressesLocation), storageType).toString
      )
      println("gcAddressesLocation: " + gcAddressesLocation)
    } else {
      // reproducing a previous run
      // TODO(johnnyaug): the server should generate these paths
      runID = UUID.randomUUID().toString
      gcCommitsLocation =
        s"${storageNSForHadoopFS.stripSuffix("/")}/_lakefs/retention/gc/commits/run_id=$runIDToReproduce/commits.csv"
      gcAddressesLocation =
        s"${storageNSForHadoopFS.stripSuffix("/")}/_lakefs/retention/gc/addresses/"
    }
    println("apiURL: " + apiURL)

    val numCommitPartitions =
      hc.getInt(LAKEFS_CONF_GC_NUM_COMMIT_PARTITIONS, DEFAULT_LAKEFS_CONF_GC_NUM_COMMIT_PARTITIONS)
    val numRangePartitions =
      hc.getInt(LAKEFS_CONF_GC_NUM_RANGE_PARTITIONS, DEFAULT_LAKEFS_CONF_GC_NUM_RANGE_PARTITIONS)
    val numAddressPartitions = hc.getInt(LAKEFS_CONF_GC_NUM_ADDRESS_PARTITIONS,
                                         DEFAULT_LAKEFS_CONF_GC_NUM_ADDRESS_PARTITIONS
                                        )
    val approxNumRangesToSpreadPerPartition = hc.getDouble(
      LAKEFS_CONF_GC_APPROX_NUM_RANGES_PER_PARTITION,
      DEFAULT_LAKEFS_CONF_GC_APPROX_NUM_RANGES_PER_PARTITION
    )
    val sampleFraction = hc.getDouble(LAKEFS_CONF_DEBUG_GC_SAMPLE_FRACTION, 1.0)

    val expiredAddresses = gc
      .getExpiredAddresses(repo,
                           storageNS,
                           gcCommitsLocation,
                           numCommitPartitions,
                           numRangePartitions,
                           numAddressPartitions,
                           approxNumRangesToSpreadPerPartition,
                           sampleFraction
                          )
      .toDF("address")
      .withColumn(MARK_ID_KEY, lit(markID))
      .persist(StorageLevel.MEMORY_ONLY_SER)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    expiredAddresses.write
      .partitionBy(MARK_ID_KEY)
      .mode(SaveMode.Overwrite)
      .parquet(gcAddressesLocation)

    println(f"Total expired addresses: ${expiredAddresses.count()}")
    println("Expired addresses:")
//    expiredAddresses.show()

    if (hc.getBoolean(LAKEFS_CONF_GC_WRITE_EXPIRED_AS_TEXT, true)) {
      // Enable source for rclone backup and resource
      // write expired addresses as text - output to '.../addresses_path+".text"'
      val gcAddressesPath = new Path(gcAddressesLocation)
      val gcTextAddressesPath =
        new Path(gcAddressesPath.getParent, gcAddressesPath.getName + ".text")
      expiredAddresses.write
        .partitionBy(MARK_ID_KEY)
        .mode(SaveMode.Overwrite)
        .text(gcTextAddressesPath.toString)
    }

    writeAddressesMarkMetadata(runID, markID, gcAddressesLocation, gcCommitsLocation)

    (gcAddressesLocation, gcCommitsLocation, expiredAddresses, runID)
  }

  def getStorageNSForSdkClient(apiClient: ApiClient, repo: String): String = {
    // The remove operation uses an SDK client to directly access the underlying storage, and therefore does not need
    // a translated storage namespace that triggers processing by Hadoop FileSystems.
    var storageNSForSdkClient = apiClient.getStorageNamespace(repo, StorageClientType.SDKClient)
    if (!storageNSForSdkClient.endsWith("/")) {
      storageNSForSdkClient += "/"
    }
    storageNSForSdkClient
  }

  def getRegion(args: Array[String]): String = {
    if (args.length == 2) args(1) else null
  }

  private def readExpiredAddresses(addressesLocation: String, markID: String): DataFrame = {
    spark.read
      .parquet(s"$addressesLocation/$MARK_ID_KEY=$markID")
      .withColumn(MARK_ID_KEY, lit(markID))
  }

  private def writeReports(
      storageNSForHadoopFS: String,
      gcRules: String,
      runID: String,
      markID: String,
      commitsDF: DataFrame,
      expiredAddresses: DataFrame,
      removed: DataFrame,
      configMapper: ConfigMapper
  ) = {
    val reportLogsDst = concatToGCLogsPrefix(storageNSForHadoopFS, "summary")
    val deletedObjectsDst = concatToGCLogsPrefix(storageNSForHadoopFS, "deleted_objects")

    val time = DateTimeFormatter.ISO_INSTANT.format(java.time.Clock.systemUTC.instant())
    writeParquetReport(commitsDF, reportLogsDst, time, "commits.parquet")

    val removedCount = removed.count()
    println(s"Total objects to delete (some may already have been deleted): ${removedCount}")
    writeJsonSummary(configMapper, reportLogsDst, removedCount, gcRules, time)
    removed
      .withColumn(MARK_ID_KEY, lit(markID))
      .withColumn(RUN_ID_KEY, lit(runID))
      .write
      .partitionBy(MARK_ID_KEY, RUN_ID_KEY)
      .mode(SaveMode.Overwrite)
      .parquet(f"${deletedObjectsDst}/dt=$time")
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
      configMapper: ConfigMapper,
      readKeysDF: DataFrame,
      storageNamespace: String,
      region: String,
      storageType: String
  ): Dataset[String] = {
    import spark.implicits._
    val bulkRemover =
      BulkRemoverFactory(storageType, configMapper.configuration, storageNamespace, region)
    val bulkSize = bulkRemover.getMaxBulkSize()
    val repartitionedKeys = repartitionBySize(readKeysDF, bulkSize, "address")
    val bulkedKeyStrings = repartitionedKeys
      .select("address")
      .map(_.getString(0)) // get address as string (address is in index 0 of row)

    bulkedKeyStrings
      .mapPartitions(iter => {
        // mapPartitions lambda executions are sent over to Spark executors, the executors don't have access to the
        // bulkRemover created above because it was created on the driver and it is not a serializable object. Therefore,
        // we create new bulkRemovers.
        val bulkRemover =
          BulkRemoverFactory(storageType, configMapper.configuration, storageNamespace, region)
        iter
          .grouped(bulkSize)
          .flatMap(bulkRemover.deleteObjects(_, storageNamespace))
      })
  }

  def remove(
      configMapper: ConfigMapper,
      storageNamespace: String,
      addressDFLocation: String,
      expiredAddresses: Dataset[Row],
      markID: String,
      region: String,
      storageType: String,
      schema: StructType
  ) = {
    println("addressDFLocation: " + addressDFLocation)

    val df = expiredAddresses.where(col(MARK_ID_KEY) === markID)
    bulkRemove(configMapper, df, storageNamespace, region, storageType)
      .toDF(schema.fieldNames: _*)
  }

  private def getMetadataMarkLocation(markId: String, gcAddressesLocation: String) = {
    s"$gcAddressesLocation/$markId.meta"
  }

  private def getMarkMetadata(markId: String, gcAddressesLocation: String): DataFrame = {
    val addressesMarkMetadataLocation = getMetadataMarkLocation(markId, gcAddressesLocation)
    spark.read.json(addressesMarkMetadataLocation)
  }

  private def populateRunIDAndCommitsLocation(
      markID: String,
      gcAddressesLocation: String
  ): Array[String] = {
    import spark.implicits._
    val markMetadataDF = getMarkMetadata(markID, gcAddressesLocation)
    markMetadataDF
      .select(RUN_ID_KEY, COMMITS_LOCATION_KEY)
      .map(row => {
        var arr = Array[String]()
        for (i <- 0 until row.length) {
          arr = arr :+ row.getString(i)
        }
        arr
      })
      .first()
  }

  private def generateMarkMetadataDataframe(runId: String, commitsLocation: String): DataFrame = {
    val metadata = Seq((runId, commitsLocation))
    val fields = Array(
      StructField(RUN_ID_KEY, StringType, nullable = false),
      StructField(COMMITS_LOCATION_KEY, StringType, nullable = false)
    )
    val schema = StructType(fields)
    spark.createDataFrame(metadata).toDF(schema.fieldNames: _*)
  }

  private def writeAddressesMarkMetadata(
      runID: String,
      markId: String,
      gcAddressesLocation: String,
      gcCommitsLocation: String
  ) = {
    generateMarkMetadataDataframe(runID, gcCommitsLocation).write
      .mode(SaveMode.Overwrite)
      .json(getMetadataMarkLocation(markId, gcAddressesLocation))
  }

  private def getAddressesLocation(storageNSForHadoopFS: String): String = {
    // TODO(jonathan): the server should generate this path
    s"${storageNSForHadoopFS.stripSuffix("/")}/_lakefs/retention/gc/addresses/"
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
      configMapper: ConfigMapper,
      dstRoot: String,
      numDeletedObjects: Long,
      gcRules: String,
      time: String
  ) = {
    val dstPath = new Path(s"${dstRoot}/dt=${time}/summary.json")
    val dstFS = dstPath.getFileSystem(configMapper.configuration)
    val jsonSummary = JObject("gc_rules" -> gcRules, "num_deleted_objects" -> numDeletedObjects)

    val stream = dstFS.create(dstPath)
    try {
      val bytes = compact(render(jsonSummary)).getBytes("UTF-8")
      stream.write(bytes)
    } finally {
      stream.close()
    }
  }

  def writeJsonSummaryForTesting = writeJsonSummary _
}
