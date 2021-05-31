package io.treeverse.clients
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{FileNotFoundException, IOException, Serializable}
import java.net.URI
import java.time.format.DateTimeFormatter
import java.util.concurrent.{Callable, ExecutorService, Executors}
import scala.util.Random
import scala.collection.JavaConverters._
import org.apache.spark._
import SparkContext._

object ExportBranch extends App {
  override def main(args: Array[String]) {
    if (args.length != 3) {
      Console.err.println("Usage: ... <repo_name> <branch_id> s3://path/to/output/du")
      System.exit(1)
    }

    val repo = args(0)
    val branch = args(1)
    val rootLocation = args(2)

    val factory = new ExporterFactory()
    val exporter = factory.CreateExporter(repo, rootLocation)

    exporter.exportAllFromBranch(branch)
  }
}

object ExportCommit extends App {
  override def main(args: Array[String]) {
    if (args.length != 3) {
      Console.err.println("Usage: ... <repo_name> <commit_id> s3://path/to/output/du")
      System.exit(1)
    }

    val repo = args(0)
    val commit_id = args(1)
    val rootLocation = args(2)

    val factory = new ExporterFactory()
    val exporter = factory.CreateExporter(repo, rootLocation)

    exporter.exportAllFromCommit(commit_id)
  }
}

object ExportFrom extends App {
  override def main(args: Array[String]) {
    if (args.length != 4) {
      Console.err.println("Usage: ... <repo_name> <branch_id> <prev_commit_id> s3://path/to/output/du")
      System.exit(1)
    }

    val repo = args(0)
    val branch = args(1)
    val prevCommitID = args(2)
    val rootLocation = args(3)

    val factory = new ExporterFactory()
    val exporter = factory.CreateExporter(repo, rootLocation)

   exporter.exportFrom(branch, prevCommitID)
  }
}

class ExporterFactory {
  def CreateExporter(repo: String, rootLocation: String): Exporter = {
    val spark = SparkSession.builder().getOrCreate()

    val sc = spark.sparkContext

    val endpoint = sc.hadoopConfiguration.get("lakefs.api.url")
    val accessKey = sc.hadoopConfiguration.get("lakefs.api.access_key")
    val secretKey = sc.hadoopConfiguration.get("lakefs.api.secret_key")

    val apiClient = new ApiClient(endpoint, accessKey, secretKey)
    new Exporter(spark, apiClient, repo, rootLocation)
  }
}
