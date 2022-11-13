package io.treeverse.gc

import io.treeverse.clients.APIConfigurations
import io.treeverse.clients.ApiClient
import io.treeverse.clients.LakeFSContext._
import io.treeverse.clients.StorageClientType
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object UncommittedGarbageCollector {
  final val UNCOMMITTED_GC_SOURCE_NAME = "uncommitted_gc"

  // lazy val spark = SparkSession.builder().appName("UncommittedGarbageCollector").getOrCreate()
  lazy val spark =
    SparkSession.builder().master("local[8]").appName("UncommittedGarbageCollector").getOrCreate()

  def getDataLocation(storageNamespace: String): String = {
    return s"${storageNamespace}/"
  }

  def main(args: Array[String]) {
    val repo = args(0)
    val hc = spark.sparkContext.hadoopConfiguration
    // val apiURL = sys.env.get("LAKEFS_CONF_API_URL").get // TODO remove commented lines
    // val accessKey = sys.env.get("LAKEFS_CONF_API_ACCESS_KEY").get
    // val secretKey = sys.env.get("LAKEFS_CONF_API_SECRET_KEY").get
    // hc.set(LAKEFS_CONF_API_URL_KEY, apiURL)
    // hc.set(LAKEFS_CONF_API_ACCESS_KEY_KEY, accessKey)
    // hc.set(LAKEFS_CONF_API_SECRET_KEY_KEY, secretKey)
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

    var storageNamespace = apiClient.getStorageNamespace(repo, StorageClientType.HadoopFS)
    if (!storageNamespace.endsWith("/")) {
      storageNamespace += "/"
    }
    val dataLocation = getDataLocation(storageNamespace)
    val uncommittedLocation = args(2) // TODO use actual uncommitted location
    val outputLocation = args(1)

    val dataPath = new Path(dataLocation)
    val dataLister = new NaiveDataLister()

    val dataDF = dataLister.listData(spark, dataPath)
    val committedDF =
      new NaiveCommittedAddressLister().listCommittedAddresses(spark, storageNamespace)
    val nsFS = new Path(storageNamespace).getFileSystem(spark.sparkContext.hadoopConfiguration)
    val uncommittedDF =
      spark.read.parquet(uncommittedLocation).select("address") // TODO use actual uncommitted data
    dataDF.except(committedDF).except(uncommittedDF).write.parquet(outputLocation)
  }
}
