package io.treeverse.clients

import com.amazonaws.services.s3.AmazonS3
import io.lakefs.clients.api.model.GarbageCollectionPrepareResponse
import io.treeverse.clients.LakeFSContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

import java.net.URI
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.JavaConverters._

class ConfigMapper(val hcValues: Broadcast[Array[(String, String)]]) extends Serializable {
  @transient lazy val configuration = {
    val conf = new Configuration()
    hcValues.value.foreach({ case (k, v) => conf.set(k, v) })
    conf
  }
}

trait RangeGetter extends Serializable {

  /** @return all rangeIDs in metarange of commitID on repo.
   */
  def getRangeIDs(commitID: String, repo: String): Iterator[String]

  /** @return all object addresses in range rangeID on repo
   */
  def getRangeAddresses(rangeID: String, repo: String): Iterator[String]
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

  def getRangeAddresses(rangeID: String, repo: String): Iterator[String] = {
    val location = ApiClient
      .get(apiConf)
      .getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(configMapper.configuration, location)
      .newIterator()
      .filter(_.message.addressType.isRelative)
      .map(a => a.message.address)
  }
}

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

class GarbageCollector(val rangeGetter: RangeGetter, configMap: ConfigMapper) extends Serializable {
  @transient lazy val spark = SparkSession.active

  def getCommitsDF(runID: String, commitDFLocation: String): Dataset[Row] = {
    spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(commitDFLocation)
  }

  def getRangeIDsForCommits(commitIDs: Dataset[String], repo: String): Dataset[String] = {
    import spark.implicits._

    commitIDs.flatMap(rangeGetter.getRangeIDs(_, repo))
  }

  val bitwiseOR = (a: Int, b: Int) => a | b

  /** Compute a distinct antijoin using  partitioner.  This is the same as
   *
   *  <pre>
   *     left.distinct.except(right.distinct)
   *  </pre>
   *
   *  but forces our plan.  Useful when Spark is unable to predict the
   *  structure and/or size of either argument.
   *
   *  @return all elements in left that are not in right.
   */
  def minus(
      left: Dataset[String],
      right: Dataset[String],
      partitioner: Partitioner
  ): Dataset[String] = {
    import spark.implicits._
    def mark(f: Int) = (p: Any) => p match { case (t: String) => (t, f) }
    val both = left.map(mark(1)).union(right.map(mark(2)))
    // For every potential output element in left.union(right), compute the
    // bitwise OR of its values in both.  This will be:
    //
    //     1 if it appears only on left;
    //     2 if it appears only on right;
    //     3 if it appears on both left and right;
    val reduced = both.rdd.reduceByKey(partitioner, bitwiseOR)
    reduced
      .filter({ case (_, y) => y == 1 })
      .map({ case (x, _) => x })
      .toDS
  }

  def getAddressesToDelete(
      expiredRangeIDs: Dataset[String],
      keepRangeIDs: Dataset[String],
      repo: String,
      numRangePartitions: Int,
      numAddressPartitions: Int
  ): Dataset[String] = {
    import spark.implicits._

    // TODO(ariels): Still appears to run on too few executors.  Might
    // repartition ahead of time.
    println(s"getAddressesToDelete: use $numRangePartitions partitions for ranges")
    val rangePartitioner = new HashPartitioner(numRangePartitions)

    // If a rangeID is exactly "kept", it need not be expanded!
    val cleanExpiredRangeIDs = minus(expiredRangeIDs, keepRangeIDs, rangePartitioner)

    val expiredAddresses = cleanExpiredRangeIDs
      .repartition(numAddressPartitions)
      .flatMap(rangeGetter.getRangeAddresses(_, repo))

    val keepAddresses =
      keepRangeIDs.repartition(numAddressPartitions).flatMap(rangeGetter.getRangeAddresses(_, repo))

    println(s"getAddressesToDelete: use $numAddressPartitions partitions for addresses")
    val addressPartitioner = new HashPartitioner(numAddressPartitions)
    minus(expiredAddresses, keepAddresses, addressPartitioner)
  }

  def getExpiredAddresses(
      repo: String,
      runID: String,
      commitDFLocation: String,
      numRangePartitions: Int,
      numAddressPartitions: Int
  ): Dataset[String] = {
    import spark.implicits._

    val commitsDF = getCommitsDF(runID, commitDFLocation)

    val keepCommitsDF = commitsDF.where("!expired").select("commit_id").as[String]
    val expiredCommitsDF = commitsDF.where("expired").select("commit_id").as[String]

    val keepRangeIDsDF = getRangeIDsForCommits(keepCommitsDF, repo)
    val expiredRangeIDsDF = getRangeIDsForCommits(expiredCommitsDF, repo)

    getAddressesToDelete(expiredRangeIDsDF,
                         keepRangeIDsDF,
                         repo,
                         numRangePartitions,
                         numAddressPartitions
                        )
  }
}

object GarbageCollector {
  lazy val spark = SparkSession.builder().appName("GarbageCollector").getOrCreate()

  def getHadoopConfigurationValues(hc: Configuration, prefixes: String*) =
    hc.iterator.asScala
      .filter(c => prefixes.exists(c.getKey.startsWith))
      .map(entry => (entry.getKey, entry.getValue))
      .toArray

  /** @return a serializable summary of values in hc starting with prefix.
   */
  def getHadoopConfigMapper(hc: Configuration, prefixes: String*): ConfigMapper =
    new ConfigMapper(spark.sparkContext.broadcast(getHadoopConfigurationValues(hc, prefixes: _*)))

  def main(args: Array[String]) {
    val hc = spark.sparkContext.hadoopConfiguration

    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val connectionTimeout = hc.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY)
    val readTimeout = hc.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
    val maxCommitIsoDatetime = hc.get(LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_ISO_DATETIME_KEY, "")
    val runIDToReproduce = hc.get(LAKEFS_CONF_DEBUG_GC_REPRODUCE_RUN_ID_KEY, "")
    if (!maxCommitIsoDatetime.isEmpty) {
      hc.setLong(
        LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_EPOCH_SECONDS_KEY,
        LocalDateTime
          .parse(hc.get(LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_ISO_DATETIME_KEY),
                 DateTimeFormatter.ISO_DATE_TIME
                )
          .toEpochSecond(ZoneOffset.UTC)
      )
    }
    val apiConf = APIConfigurations(apiURL, accessKey, secretKey, connectionTimeout, readTimeout)
    val apiClient = ApiClient.get(apiConf)
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
    val hcValues = spark.sparkContext.broadcast(getHadoopConfigurationValues(hc, "fs.", "lakefs."))

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
    var storageNSForHadoopFS = apiClient.getStorageNamespace(repo, StorageClientType.HadoopFS)
    if (!storageNSForHadoopFS.endsWith("/")) {
      storageNSForHadoopFS += "/"
    }
    var prepareResult: GarbageCollectionPrepareResponse = null
    var runID = ""
    var gcCommitsLocation = ""
    var gcAddressesLocation = ""
    if (runIDToReproduce == "") {
      prepareResult = apiClient.prepareGarbageCollectionCommits(repo, previousRunID)
      runID = prepareResult.getRunId
      gcCommitsLocation =
        ApiClient.translateURI(new URI(prepareResult.getGcCommitsLocation), storageType).toString
      println("gcCommitsLocation: " + gcCommitsLocation)
      gcAddressesLocation =
        ApiClient.translateURI(new URI(prepareResult.getGcAddressesLocation), storageType).toString
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

    val configMapper = new ConfigMapper(hcValues)
    val gc = new GarbageCollector(new LakeFSRangeGetter(apiConf, configMapper), configMapper)

    val numRangePartitions =
      hc.getInt(LAKEFS_CONF_GC_NUM_RANGE_PARTITIONS, DEFAULT_LAKEFS_CONF_GC_NUM_RANGE_PARTITIONS)
    val numAddressPartitions = hc.getInt(LAKEFS_CONF_GC_NUM_ADDRESS_PARTITIONS,
                                         DEFAULT_LAKEFS_CONF_GC_NUM_ADDRESS_PARTITIONS
                                        )

    val expiredAddresses = gc
      .getExpiredAddresses(repo, runID, gcCommitsLocation, numRangePartitions, numAddressPartitions)
      .toDF("address")
      .withColumn("run_id", lit(runID))

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

    val schema = StructType(Array(StructField("addresses", StringType, nullable = false)))
    val removed =
      if (hc.getBoolean(LAKEFS_CONF_DEBUG_GC_NO_DELETE_KEY, false)) {
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      } else
        remove(configMapper,
               storageNSForSdkClient,
               gcAddressesLocation,
               expiredAddresses,
               runID,
               region,
               storageType,
               schema
              )

    val commitsDF = gc.getCommitsDF(runID, gcCommitsLocation)
    writeReports(storageNSForHadoopFS,
                 gcRules,
                 runID,
                 commitsDF,
                 expiredAddresses,
                 removed,
                 configMapper
                )

    spark.close()
  }

  private def writeReports(
      storageNSForHadoopFS: String,
      gcRules: String,
      runID: String,
      commitsDF: DataFrame,
      expiredAddresses: DataFrame,
      removed: DataFrame,
      configMapper: ConfigMapper
  ) = {
    val reportLogsDst = concatToGCLogsPrefix(storageNSForHadoopFS, "summary")
    val reportExpiredDst = concatToGCLogsPrefix(storageNSForHadoopFS, "expired_addresses")
    val time = DateTimeFormatter.ISO_INSTANT.format(java.time.Clock.systemUTC.instant())
    writeParquetReport(commitsDF, reportLogsDst, time, "commits.parquet")
    writeParquetReport(expiredAddresses, reportExpiredDst, time)
    writeJsonSummary(configMapper, reportLogsDst, removed.count(), gcRules, time)

    removed
      .withColumn("run_id", lit(runID))
      .write
      .partitionBy("run_id")
      .mode(SaveMode.Overwrite)
      .parquet(
        concatToGCLogsPrefix(storageNSForHadoopFS, s"deleted_objects/$time/deleted.parquet")
      )
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
        // bulkRemover created above because it was created on the driver and it is not a serializeable object. Therefore,
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
      runID: String,
      region: String,
      storageType: String,
      schema: StructType
  ) = {
    println("addressDFLocation: " + addressDFLocation)

    val df = expiredAddresses.where(col("run_id") === runID)
    bulkRemove(configMapper, df, storageNamespace, region, storageType).toDF(schema.fieldNames: _*)
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
      stream.writeChars(compact(render(jsonSummary)))
    } finally {
      stream.close()
    }
  }
}
