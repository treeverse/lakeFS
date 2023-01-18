package io.treeverse.gc

import io.treeverse.clients.APIConfigurations
import io.treeverse.clients.ApiClient
import io.treeverse.clients.GarbageCollector._
import io.treeverse.clients.LakeFSContext._
import io.treeverse.clients._
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

import java.net.URI
import java.util.Date
import java.time.format.DateTimeFormatter

object UncommittedGarbageCollector {
  final val UNCOMMITTED_GC_SOURCE_NAME = "uncommitted_gc"
  private final val DATA_PREFIX = "data/"

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("UncommittedGarbageCollector")
      .getOrCreate()

  // exclude list of old data location
  private val excludeFromOldData = Seq("dummy")

  /** list repository objects directly from object store.
   *  Reads the objects from both old repository structure and new repository structure
   *
   *  @param storageNamespace The storageNamespace to read from
   *  @param before Exclude objects which last_modified date is newer than before Date
   *  @return DF listing all objects under given storageNamespace
   */
  def listObjects(storageNamespace: String, before: Date): DataFrame = {
    // TODO(niro): parallelize reads from root and data paths
    val sc = spark.sparkContext
    val oldDataPath = new Path(storageNamespace)
    val dataPath = new Path(storageNamespace, DATA_PREFIX)

    val configMapper = new ConfigMapper(
      sc.broadcast(
        HadoopUtils.getHadoopConfigurationValues(sc.hadoopConfiguration, "fs.", "lakefs.")
      )
    )
    // Read objects from data path (new repository structure)
    var dataDF = new ParallelDataLister().listData(configMapper, dataPath)
    dataDF = dataDF
      .withColumn(
        "address",
        concat(lit(DATA_PREFIX), col("base_address"))
      )

    // Read objects from namespace root, for old structured repositories

    // TODO (niro): implement parallel lister for old repositories (https://github.com/treeverse/lakeFS/issues/4620)
    val oldDataDF = new NaiveDataLister()
      .listData(configMapper, oldDataPath)
      .withColumn("address", col("base_address"))
      .filter(!col("address").isin(excludeFromOldData: _*))
    dataDF = dataDF.union(oldDataDF).filter(col("last_modified") < before.getTime)

    dataDF
  }

  def getFirstSlice(dataDF: DataFrame, repo: String): String = {
    var firstSlice = ""
    // Need the before filter to to exclude slices that are not actually read
    val slices =
      dataDF.filter(col("address").startsWith(DATA_PREFIX) && !col("base_address").startsWith(repo))
    if (!slices.isEmpty) {
      firstSlice = slices.first.getAs[String]("base_address").split("/")(0)
    }
    firstSlice
  }

  def validateRunModeConfigs(
      shouldMark: Boolean,
      shouldSweep: Boolean,
      markID: String
  ): Unit = {
    if (!shouldMark && !shouldSweep) {
      throw new ParameterValidationException(
        "Nothing to do, must specify at least one of mark, sweep. Exiting..."
      )
    }
    if (!shouldMark && markID.isEmpty) { // Sweep-only mode but no mark ID to sweep
      throw new ParameterValidationException(
        s"Please provide a mark ID ($LAKEFS_CONF_GC_MARK_ID) for sweep-only mode. Exiting...\n"
      )
    }
    if (shouldMark && markID.nonEmpty) {
      throw new ParameterValidationException("Can't provide mark ID for mark mode. Exiting...")
    }
  }

  def main(args: Array[String]): Unit = {
    var runID = ""
    var firstSlice = ""
    var success = false
    var markedAddresses = spark.emptyDataFrame.withColumn("address", lit(""))
    var addressesToDelete = spark.emptyDataFrame.withColumn("address", lit(""))
    val repo = args(0)
    val hc = spark.sparkContext.hadoopConfiguration
    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val connectionTimeout = hc.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY)
    val readTimeout = hc.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
    val minAgeStr = hc.get(LAKEFS_CONF_DEBUG_GC_UNCOMMITTED_MIN_AGE_SECONDS_KEY)
    val minAgeSeconds = {
      if (minAgeStr != null && minAgeStr.nonEmpty && minAgeStr.toInt > 0) {
        minAgeStr.toInt
      } else
        DEFAULT_GC_UNCOMMITTED_MIN_AGE_SECONDS
    }
    val cutoffTime = DateUtils.addSeconds(new Date(), -minAgeSeconds)
    val startTime = java.time.Clock.systemUTC.instant()

    val shouldMark = hc.getBoolean(LAKEFS_CONF_GC_DO_MARK, true)
    val shouldSweep = hc.getBoolean(LAKEFS_CONF_GC_DO_SWEEP, true)
    val markID = hc.get(LAKEFS_CONF_GC_MARK_ID, "")

    validateRunModeConfigs(shouldMark, shouldSweep, markID)

    val apiConf =
      APIConfigurations(apiURL,
                        accessKey,
                        secretKey,
                        connectionTimeout,
                        readTimeout,
                        UncommittedGarbageCollector.UNCOMMITTED_GC_SOURCE_NAME
                       )
    val apiClient = ApiClient.get(apiConf)
    val storageType = apiClient.getBlockstoreType()
    var storageNamespace = apiClient.getStorageNamespace(repo, StorageClientType.HadoopFS)
    if (!storageNamespace.endsWith("/")) {
      storageNamespace += "/"
    }

    try {
      if (shouldMark) {
        // Read objects directly from object storage
        val dataDF = listObjects(storageNamespace, cutoffTime)

        // Get first Slice
        firstSlice = getFirstSlice(dataDF, repo)

        // Process uncommitted
        val uncommittedGCRunInfo =
          new APIUncommittedAddressLister(apiClient).listUncommittedAddresses(spark, repo)

        var uncommittedDF =
          if (uncommittedGCRunInfo.uncommittedLocation != "") {
            val uncommittedLocation =
              ApiClient
                .translateURI(new URI(uncommittedGCRunInfo.uncommittedLocation), storageType)
                .toString
            spark.read.parquet(uncommittedLocation)
          } else {
            // in case of no uncommitted entries
            spark.emptyDataFrame.withColumn("physical_address", lit(""))
          }

        uncommittedDF = uncommittedDF.select(uncommittedDF("physical_address").as("address"))
        runID = uncommittedGCRunInfo.runID

        // Process committed
        val clientStorageNamespace =
          apiClient.getStorageNamespace(repo, StorageClientType.SDKClient)
        val committedDF = new NaiveCommittedAddressLister()
          .listCommittedAddresses(spark, storageNamespace, clientStorageNamespace)

        addressesToDelete = dataDF
          .select("address")
          .except(committedDF)
          .except(uncommittedDF)
      }
      if (shouldSweep) {
        if (shouldMark) { // get the expired addresses from the mark id run
          markedAddresses = addressesToDelete
          println("deleting marked addresses: " + runID)
        } else {
          markedAddresses = readMarkedAddresses(storageNamespace, markID)
          println("deleting marked addresses: " + markID)
        }

        val storageNSForSdkClient = getStorageNSForSdkClient(apiClient: ApiClient, repo)
        val region = getRegion(args)
        val hcValues = spark.sparkContext.broadcast(
          HadoopUtils.getHadoopConfigurationValues(hc, "fs.", "lakefs.")
        )
        val configMapper = new ConfigMapper(hcValues)

        val removed = GarbageCollector
          .bulkRemove(configMapper, markedAddresses, storageNSForSdkClient, region, storageType)
          .toDF()
        removed.collect()
      }
      // Flow completed successfully - set success to true
      success = true
    } finally {
      if (runID.nonEmpty && shouldMark) {
        writeReports(
          storageNamespace,
          runID,
          firstSlice,
          startTime,
          cutoffTime.toInstant,
          success,
          addressesToDelete
        )
      }
      spark.close()
    }
  }

  def writeReports(
      storageNamespace: String,
      runID: String,
      firstSlice: String,
      startTime: java.time.Instant,
      startListTime: java.time.Instant,
      success: Boolean,
      expiredAddresses: DataFrame
  ): Unit = {
    val reportDst = formatRunPath(storageNamespace, runID)
    val summary =
      writeJsonSummary(reportDst,
                       runID,
                       firstSlice,
                       startTime,
                       startListTime,
                       success,
                       expiredAddresses.count()
                      )
    println(s"Report for mark_id=$runID summary=$summary")

    val cachedAddresses = expiredAddresses.cache()
    cachedAddresses.write.parquet(s"$reportDst/deleted")
    cachedAddresses.write.text(s"$reportDst/deleted.text")
  }

  private def formatRunPath(storageNamespace: String, runID: String): String = {
    s"${storageNamespace}_lakefs/retention/gc/uncommitted/$runID"
  }

  def readMarkedAddresses(storageNamespace: String, markID: String): DataFrame = {
    val reportPath = formatRunPath(storageNamespace, markID) + "/summary.json"
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path(reportPath))) {
      throw new FailedRunException(s"Mark ID ($markID) does not exist")
    }
    val markedRunSummary = spark.read.json(reportPath)
    if (!markedRunSummary.first.getAs[Boolean]("success")) {
      throw new FailedRunException(s"Provided mark ($markID) is of a failed run")
    } else {
      spark.read.parquet(s"$reportPath/deleted")
    }
  }

  def writeJsonSummary(
      dst: String,
      runID: String,
      firstSlice: String,
      startTime: java.time.Instant,
      startListTime: java.time.Instant,
      success: Boolean,
      numDeletedObjects: Long
  ): String = {
    val dstPath = new Path(s"$dst/summary.json")
    val dstFS = dstPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val jsonSummary = JObject(
      "run_id" -> runID,
      "success" -> success,
      "first_slice" -> firstSlice,
      "start_time" -> DateTimeFormatter.ISO_INSTANT.format(startTime),
      "start_list_time" -> DateTimeFormatter.ISO_INSTANT.format(startListTime),
      "num_deleted_objects" -> numDeletedObjects
    )
    val summary = compact(render(jsonSummary))
    val stream = dstFS.create(dstPath)
    try {
      stream.writeBytes(summary)
    } finally {
      stream.close()
    }
    summary
  }
}
