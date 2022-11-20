package io.treeverse.gc

import io.treeverse.clients.APIConfigurations
import io.treeverse.clients.ApiClient
import io.treeverse.clients.LakeFSContext._
import io.treeverse.clients.StorageClientType
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.util.Date
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions._
import io.treeverse.clients.ConfigMapper
import io.treeverse.clients.HadoopUtils

object UncommittedGarbageCollector {
  final val UNCOMMITTED_GC_SOURCE_NAME = "uncommitted_gc"

  lazy val spark = SparkSession.builder().appName("UncommittedGarbageCollector").getOrCreate()

  def getDataLocation(storageNamespace: String): String = {
    return s"${storageNamespace}/"
  }
  def getAddressesToDelete(
      apiClient: ApiClient,
      repoName: String,
      uncommittedDF: DataFrame,
      excludedDF: DataFrame,
      before: Date
  ): DataFrame = {
    var storageNamespace = apiClient.getStorageNamespace(repoName, StorageClientType.HadoopFS)
    if (!storageNamespace.endsWith("/")) {
      storageNamespace += "/"
    }
    val sc = spark.sparkContext
    val dataLocation = getDataLocation(storageNamespace)
    val dataPath = new Path(dataLocation)
    val configMapper = new ConfigMapper(
      sc.broadcast(
        HadoopUtils.getHadoopConfigurationValues(sc.hadoopConfiguration, "fs.", "lakefs.")
      )
    )
    var dataDF = new NaiveDataLister().listData(configMapper, dataPath)
    dataDF = dataDF.filter(dataDF("last_modified") < before.getTime()).select("address")
    val nsFS = new Path(storageNamespace).getFileSystem(sc.hadoopConfiguration)
    val committedDF =
      new NaiveCommittedAddressLister().listCommittedAddresses(spark, storageNamespace)
    dataDF
      .except(excludedDF)
      .except(committedDF)
      .except(uncommittedDF)
  }

  def main(args: Array[String]) {
    val repo = args(0)
    val hc = spark.sparkContext.hadoopConfiguration
    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val connectionTimeout = hc.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY)
    val readTimeout = hc.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
    val apiConf =
      APIConfigurations(apiURL,
                        accessKey,
                        secretKey,
                        connectionTimeout,
                        readTimeout,
                        UncommittedGarbageCollector.UNCOMMITTED_GC_SOURCE_NAME
                       )
    val apiClient = ApiClient.get(apiConf)
    val uncommittedLocation = args(2) // TODO use actual uncommitted location
    val outputLocation = args(1)
    val uncommittedGCRunInfo =
      new DummyUncommittedAddressLister(uncommittedLocation).listUncommittedAddresses(spark, repo)
    val uncommittedDF = spark.read.parquet(uncommittedGCRunInfo.uncommitedLocation)
    val addressesToDelete =
      getAddressesToDelete(apiClient,
                           repo,
                           uncommittedDF,
                           spark.emptyDataFrame.withColumn("address", lit("")),
                           DateUtils.addHours(new Date(), -6)
                          )
    addressesToDelete.write.parquet(outputLocation)
  }
}
