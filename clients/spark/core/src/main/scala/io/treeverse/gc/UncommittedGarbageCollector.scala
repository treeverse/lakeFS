package io.treeverse.gc

import io.treeverse.clients.APIConfigurations
import io.treeverse.clients.ApiClient
import io.treeverse.clients.LakeFSContext._
import io.treeverse.clients.StorageClientType
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Date
import java.util.UUID
import java.time.format.DateTimeFormatter
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions._
import io.treeverse.clients.ConfigMapper
import io.treeverse.clients.HadoopUtils
import io.treeverse.clients.GarbageCollector

object UncommittedGarbageCollector {
  final val UNCOMMITTED_GC_SOURCE_NAME = "uncommitted_gc"
  lazy val spark: SparkSession =
    SparkSession.builder().appName("UncommittedGarbageCollector").getOrCreate()

  def listObjects(storageNamespace: String, before: Date): DataFrame = {
    val sc = spark.sparkContext
    val dataLocation = storageNamespace + "data" // TODO(niro): handle better
    val dataPath = new Path(dataLocation)
    val configMapper = new ConfigMapper(
      sc.broadcast(
        HadoopUtils.getHadoopConfigurationValues(sc.hadoopConfiguration, "fs.", "lakefs.")
      )
    )
    var dataDF = new NaiveDataLister().listData(configMapper, dataPath)
    dataDF = dataDF.filter(dataDF("last_modified") < before.getTime).select("address")
    dataDF
  }

  def main(args: Array[String]): Unit = {
    var runID = ""
    var firstSlice = ""
    var success = true
    var removed = spark.emptyDataFrame.withColumn("address", lit(""))
    var addressesToDelete = spark.emptyDataFrame.withColumn("address", lit(""))
    val repo = args(0)
    val hc = spark.sparkContext.hadoopConfiguration
    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val connectionTimeout = hc.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY)
    val readTimeout = hc.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
    val startTime = java.time.Clock.systemUTC.instant()

    val shouldMark = hc.getBoolean(LAKEFS_CONF_GC_DO_MARK, true)
    val shouldSweep = hc.getBoolean(LAKEFS_CONF_GC_DO_SWEEP, true)
    val markID = hc.get(LAKEFS_CONF_GC_MARK_ID, UUID.randomUUID().toString)

    GarbageCollector.validateRunModeConfigs(hc.getBoolean(LAKEFS_CONF_DEBUG_GC_NO_DELETE_KEY, false),
      shouldMark,
      shouldSweep,
      hc.get(LAKEFS_CONF_GC_MARK_ID, "")
    )

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
        val dataDF = listObjects(storageNamespace, DateUtils.addHours(new Date(), -6))
        val uncommittedGCRunInfo =
          new APIUncommittedAddressLister(apiClient).listUncommittedAddresses(spark, repo)
        var uncommittedDF =
          if (uncommittedGCRunInfo.uncommittedLocation != "")
            spark.read.parquet(uncommittedGCRunInfo.uncommittedLocation)
          else {
            // in case of no uncommitted entries
            spark.emptyDataFrame.withColumn("physical_address", lit(""))
          }
        uncommittedDF = uncommittedDF.select(uncommittedDF("physical_address").as("address"))
        runID = uncommittedGCRunInfo.runID

        val committedDF =
          new NaiveCommittedAddressLister().listCommittedAddresses(spark, storageNamespace)
        addressesToDelete = dataDF
          .except(spark.emptyDataFrame.withColumn("address", lit("")))
          .except(committedDF)
          .except(uncommittedDF)
//        val firstFile = dataDF.select(col("address")).first().toString()
//        firstSlice = firstFile.substring(0, firstFile.lastIndexOf("/"))
      }

      removed = {
        if (shouldSweep) {
          if (markID != "") { // get the expired addresses from the mark id run
            addressesToDelete = readMarkedAddresses(storageNamespace, markID)
          }
          if (!shouldMark) { //todo
            runID = "sweep" + markID
          }

//          val region = if (args.length == 2) args(1) else null
          val region = "us-east-2"
          // The remove operation uses an SDK client to directly access the underlying storage, and therefore does not need
          // a translated storage namespace that triggers processing by Hadoop FileSystems.
          var storageNSForSdkClient = apiClient.getStorageNamespace(repo, StorageClientType.SDKClient)
          if (!storageNSForSdkClient.endsWith("/")) {
            storageNSForSdkClient += "/"
          }

          // delete??
          val hcValues = spark.sparkContext.broadcast(HadoopUtils.getHadoopConfigurationValues(hc, "fs.", "lakefs."))
          val configMapper = new ConfigMapper(hcValues)

          GarbageCollector.bulkRemove(configMapper, addressesToDelete, storageNamespace, region, storageType).toDF()
        } else {
          spark.emptyDataFrame.withColumn("address", lit(""))
        }
      }
    } catch {
      case e: Throwable =>
        success = false
        throw e
    } finally {
      writeReports(
        storageNamespace,
        runID,
        firstSlice,
        startTime,
        success,
        addressesToDelete,
        removed
      )
      spark.close()
    }
  }

  def writeReports(
      storageNamespace: String,
      runID: String,
      firstSlice: String,
      startTime: java.time.Instant,
      success: Boolean,
      expiredAddresses: DataFrame,
      removed: DataFrame
  ): Unit = {
    val reportDst = reportPath(storageNamespace, runID)
    writeJsonSummary(reportDst, runID, firstSlice, startTime, success, removed.count())

    expiredAddresses.write.parquet(s"$reportDst/marked")
    removed.write.parquet(s"$reportDst/deleted")
  }

  private def reportPath(storageNamespace: String, runID: String): String = {
    val report = s"${storageNamespace}_lakefs/retention/gc/uncommitted/$runID"
    return report
  }

  private def readMarkedAddresses(storageNamespace: String, markID: String): DataFrame = {
    // if last run error ??
    val path = reportPath(storageNamespace, markID)
    spark.read.parquet(s"$path/marked")
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
