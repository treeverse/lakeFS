package io.treeverse.gc

import io.treeverse.clients.LakeFSContext._
import io.treeverse.clients._
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

import java.time.format.DateTimeFormatter
import java.util.Date

object UncommittedGarbageCollector {
  final val UNCOMMITTED_GC_SOURCE_NAME = "uncommitted_gc"

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
    val dataPrefix = "data"
    val dataPath = new Path(storageNamespace, dataPrefix) // TODO(niro): handle better

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
        concat(lit(dataPrefix), lit("/"), col("base_address"))
      )
      .select("address", "last_modified")

    // Read objects from namespace root, for old structured repositories

    // TODO (niro): implement parallel lister for old repositories (https://github.com/treeverse/lakeFS/issues/4620)
    val oldDataDF = new NaiveDataLister()
      .listData(configMapper, oldDataPath)
      .filter(!col("address").isin(excludeFromOldData: _*))
    dataDF = dataDF.union(oldDataDF).filter(col("last_modified") < before.getTime).select("address")

    dataDF
  }

  def main(args: Array[String]): Unit = {
    var runID = ""
    var firstSlice = ""
    var success = true
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

    val apiConf =
      APIConfigurations(apiURL,
                        accessKey,
                        secretKey,
                        connectionTimeout,
                        readTimeout,
                        UncommittedGarbageCollector.UNCOMMITTED_GC_SOURCE_NAME
                       )
    val apiClient = ApiClient.get(apiConf)
    var storageNamespace = apiClient.getStorageNamespace(repo, StorageClientType.HadoopFS)
    if (!storageNamespace.endsWith("/")) {
      storageNamespace += "/"
    }

    try {
      // Read objects directly from object storage
      val dataDF = listObjects(storageNamespace, cutoffTime)

      // Process uncommitted
      val uncommittedGCRunInfo =
        new APIUncommittedAddressLister(apiClient).listUncommittedAddresses(spark, repo)

      var uncommittedDF =
        if (uncommittedGCRunInfo.uncommittedLocation != "") {
          spark.read.parquet(uncommittedGCRunInfo.uncommittedLocation)
        } else {
          // in case of no uncommitted entries
          spark.emptyDataFrame.withColumn("physical_address", lit(""))
        }

      uncommittedDF = uncommittedDF.select(uncommittedDF("physical_address").as("address"))
      runID = uncommittedGCRunInfo.runID

      // Process committed
      val committedDF = new NaiveCommittedAddressLister()
        .listCommittedAddresses(spark, storageNamespace)

      addressesToDelete = dataDF
        .except(committedDF)
        .except(uncommittedDF)

      // TODO (niro): not working - need to find the most efficient way to save the first slice
      if (!dataDF.isEmpty) {
        val firstFile = dataDF.select(col("address")).first().toString()
        firstSlice = firstFile.substring(0, firstFile.lastIndexOf("/"))
      }
    } catch {
      case e: Throwable =>
        success = false
        throw e
    } finally {
      if (runID.nonEmpty) {
        writeReports(
          storageNamespace,
          runID,
          firstSlice,
          startTime,
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
      success: Boolean,
      removed: DataFrame
  ): Unit = {
    val reportDst = s"${storageNamespace}_lakefs/retention/gc/uncommitted/$runID"
    writeJsonSummary(reportDst, runID, firstSlice, startTime, success, removed.count())

    removed.write.parquet(s"$reportDst/deleted")
  }

  private def writeJsonSummary(
      dst: String,
      runID: String,
      firstSlice: String,
      startTime: java.time.Instant,
      success: Boolean,
      numDeletedObjects: Long
  ): Unit = {
    val dstPath = new Path(s"$dst/summary.json")
    val dstFS = dstPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val jsonSummary = JObject(
      "run_id" -> runID,
      "success" -> success,
      "first_slice" -> firstSlice,
      "start_time" -> DateTimeFormatter.ISO_INSTANT.format(startTime),
      "num_deleted_objects" -> numDeletedObjects
    )

    val stream = dstFS.create(dstPath)
    try {
      stream.writeBytes(compact(render(jsonSummary)))
    } finally {
      stream.close()
    }
  }
}
