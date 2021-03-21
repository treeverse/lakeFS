package io.treeverse.clients.examples

import io.treeverse.clients.{ApiClient, Exporter}
import org.apache.spark.sql.SparkSession

object Export extends App {
  override def main(args: Array[String]) {
    if (args.length != 4) {
      Console.err.println("Usage: ... <repo_name> <branch_id> <prev_commit_id> s3://path/to/output/du")
      System.exit(1)
    }

    val endpoint = "http://<LAKEFS_ENDPOINT>/api/v1"
    val accessKey = "<LAKEFS_ACCESS_KEY_ID>"
    val secretKey = "<LAKEFS_SECRET_ACCESS_KEY>"
    val s3accessKey = "<S3_ACCESS_KEY_ID>"
    val s3secretKey = "<S3_SECRET_ACCESS_KEY>"

    val repo = args(0)
    val branch = args(1)
    val prevCommitID = args(2)
    val rootLocation = args(3)

    val spark = SparkSession.builder().appName("I can list").master("local").getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("lakefs.api.url", endpoint)
    sc.hadoopConfiguration.set("lakefs.api.access_key", accessKey)
    sc.hadoopConfiguration.set("lakefs.api.secret_key", secretKey)
    sc.hadoopConfiguration.set("fs.s3a.access.key", s3accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key",s3secretKey)

    val apiClient = new ApiClient(endpoint, accessKey, secretKey)
    val exporter = new Exporter(apiClient, repo, rootLocation)

    exporter.exportAllFromCommit(spark, prevCommitID)
    exporter.exportAllFromBranch(spark, branch)
    exporter.exportFrom(spark, branch, prevCommitID)
  }
}
