package io.treeverse.gc

import io.treeverse.clients.APIConfigurations
import io.treeverse.clients.ApiClient
import io.treeverse.clients.LakeFSContext._
import io.treeverse.clients.StorageClientType
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Date
import java.time.format.DateTimeFormatter
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions._
import io.treeverse.clients.ConfigMapper
import io.treeverse.clients.HadoopUtils

object UncommittedGarbageCollector {
  final val UNCOMMITTED_GC_SOURCE_NAME = "uncommitted_gc"
  lazy val spark: SparkSession =
    SparkSession.builder().appName("UncommittedGarbageCollector").getOrCreate()

  def getDataLocation(storageNamespace: String, prefix: String): String = {
    if (prefix == "") {
      return s"$storageNamespace/"
    }
    s"$storageNamespace/$prefix/"
  }

  def listObjects(storageNamespace: String, prefix: String, before: Date): DataFrame = {
    val sc = spark.sparkContext
    val dataLocation = getDataLocation(storageNamespace, prefix)
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

  def getAddressesToDelete(
      storageNamespace: String,
      dataDF: DataFrame,
      uncommittedDF: DataFrame,
      excludedDF: DataFrame
  ): DataFrame = {

    val committedDF =
      new NaiveCommittedAddressLister().listCommittedAddresses(spark, storageNamespace)
    dataDF
      .except(excludedDF)
      .except(committedDF)
      .except(uncommittedDF)
  }

  def main(args: Array[String]): Unit = {
    var runID = ""
    var lastSlice = ""
    var success = true
    var addressesToDelete = spark.emptyDataFrame.withColumn("address", lit(""))
    val repo = args(0)
    val hc = spark.sparkContext.hadoopConfiguration
    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val connectionTimeout = hc.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY)
    val readTimeout = hc.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
    val startTime = DateTimeFormatter.ISO_INSTANT.format(java.time.Clock.systemUTC.instant())
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
      val dataDF = listObjects(storageNamespace, "data", DateUtils.addHours(new Date(), -6))
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

      val uncommittedDF = spark.read.parquet(uncommittedGCRunInfo.uncommittedLocation)

      addressesToDelete = getAddressesToDelete(
        storageNamespace,
        dataDF,
        uncommittedDF,
        spark.emptyDataFrame.withColumn("address", lit(""))
      )
      lastSlice = dataDF.select(col("address")).first().toString()
    } catch {
      case _: Throwable => success = false
    } finally {
      val markID = "mark-id" // TODO (niro): Add mark ID
      writeReports(
        storageNamespace,
        runID,
        markID,
        lastSlice,
        startTime,
        success,
        addressesToDelete
      )

      spark.close()
    }
  }

  def writeReports(
      storageNamespace: String,
      runID: String,
      markID: String,
      lastSlice: String,
      startTime: String,
      success: Boolean,
      removed: DataFrame
  ): Unit = {
    val reportDst = s"${storageNamespace}_lakefs/retention/gc/uncommitted/$runID"
    writeJsonSummary(reportDst, runID, lastSlice, startTime, success, removed.count())

    removed
      .withColumn(MARK_ID_KEY, lit(markID))
      .withColumn(RUN_ID_KEY, lit(runID))
      .write
      .partitionBy(MARK_ID_KEY, RUN_ID_KEY)
      .mode(SaveMode.Overwrite)
      .parquet(s"$reportDst/deleted.parquet")
  }

  private def writeJsonSummary(
      dst: String,
      runID: String,
      lastSlice: String,
      startTime: String,
      success: Boolean,
      numDeletedObjects: Long
  ): Unit = {
    val dstPath = new Path(s"$dst/summary.json")
    val dstFS = dstPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val jsonSummary = JObject(
      "run_id" -> runID,
      "success" -> success,
      "last_slice" -> lastSlice,
      "start_time" -> startTime,
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
