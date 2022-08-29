package io.treeverse.clients.examples

import io.treeverse.clients.{APIConfigurations, ApiClient, Exporter}
import org.apache.spark.sql.SparkSession
import io.treeverse.clients.LakeFSContext.{
  LAKEFS_CONF_API_ACCESS_KEY_KEY,
  LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY,
  LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY,
  LAKEFS_CONF_API_SECRET_KEY_KEY,
  LAKEFS_CONF_API_URL_KEY
}

// This example Export program copies all files from a lakeFS branch in a lakeFS repository
// to the specified s3 bucket. When the export ends, file structure under the bucket will match
// the one in the branch.
// This example supports continuous exports - provided with <prev_commit_id>, it will handle only the files
// that were changed since that commit and avoid copying unnecessary data.
object Export extends App {
  override def main(args: Array[String]) {
    if (args.length != 4) {
      Console.err.println(
        "Usage: ... <repo_name> <branch_id> <prev_commit_id> s3://path/to/output/du"
      )
      System.exit(1)
    }

    val endpoint = "http://<LAKEFS_ENDPOINT>/api/v1"
    val accessKey = "<LAKEFS_ACCESS_KEY_ID>"
    val secretKey = "<LAKEFS_SECRET_ACCESS_KEY>"
    val connectionTimeoutSec = "10"
    val readTimeoutSec = "10"

    val repo = args(0)
    val branch = args(1)
    val prevCommitID = args(2)
    val rootLocation = args(3)

    val spark = SparkSession.builder().appName("I can list").master("local").getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set(LAKEFS_CONF_API_URL_KEY, endpoint)
    sc.hadoopConfiguration.set(LAKEFS_CONF_API_ACCESS_KEY_KEY, accessKey)
    sc.hadoopConfiguration.set(LAKEFS_CONF_API_SECRET_KEY_KEY, secretKey)
    sc.hadoopConfiguration.set(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY, connectionTimeoutSec)
    sc.hadoopConfiguration.set(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY, readTimeoutSec)

    val apiClient = ApiClient.get(
      APIConfigurations(endpoint, accessKey, secretKey, connectionTimeoutSec, readTimeoutSec) /**/
    )
    val exporter = new Exporter(spark, apiClient, repo, rootLocation)

    exporter.exportAllFromBranch(branch)
//    exporter.exportAllFromCommit(prevCommitID)
//    exporter.exportFrom(branch, prevCommitID)

    spark.sparkContext.stop()
  }
}
