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
import org.rogach.scallop._
import io.treeverse.clients.LakeFSContext.{
  LAKEFS_CONF_API_ACCESS_KEY_KEY,
  LAKEFS_CONF_API_SECRET_KEY_KEY,
  LAKEFS_CONF_API_URL_KEY,
  LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY,
  LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY
}

class Exporter(
    spark: SparkSession,
    apiClient: ApiClient,
    filter: KeyFilter,
    repoName: String,
    dstRoot: String,
    parallelism: Int
) {
  def this(spark: SparkSession, apiClient: ApiClient, repoName: String, dstRoot: String) {
    this(spark, apiClient, new SparkFilter(), repoName, dstRoot, Exporter.defaultParallelism)
  }

  def exportAllFromBranch(branch: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    exportAllFromCommit(commitID)
  }

  def exportAllFromCommit(commitID: String): Unit = {
    val ns = apiClient.getStorageNamespace(repoName, StorageClientType.HadoopFS)
    val df = LakeFSContext.newDF(spark, repoName, commitID)

    val tableName = randPrefix() + "_commit"
    df.createOrReplaceTempView(tableName)

    // pin Exporter field to avoid serialization
    val dst = dstRoot
    val actionsDF = spark.sql(s"SELECT 'copy' as action, * FROM ${tableName}")

    actOnActions(ns, dst, commitID, actionsDF)
  }

  final private val maxLoggedErrors = 10000
  private def export(
      round: Int,
      ns: String,
      rel: String,
      commitID: String,
      actionsDF: DataFrame
  ): Boolean = {
    import spark.implicits._

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val serializedConf = new SerializableWritable(hadoopConf)
    val f = filter
    val par = parallelism
    val errs = actionsDF
      .mapPartitions(part => {
        val pool: ExecutorService = Executors.newFixedThreadPool(par)

        val res = pool
          .invokeAll(
            part.map(row => new Handler(f, round, ns, rel, serializedConf, row)).toList.asJava
          )
          .asScala
          .map(fut => fut.get())
          .iterator
        pool.shutdown()
        res
      })
      .filter(status => !status.success)

    if (errs.isEmpty) {
      return true
    }
    val count = errs.count()
    writeSummaryFile(false,
                     commitID,
                     errs
                       .sample(math.min(1.0, maxLoggedErrors.toDouble / count))
                       .map(s => s.msg)
                       .reduce(_ + "\n" + _)
                    )
    false
  }

  def exportFrom(branch: String, prevCommitID: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    val ns = apiClient.getStorageNamespace(repoName, StorageClientType.HadoopFS)

    val newDF = LakeFSContext.newDF(spark, repoName, commitID)
    val prevDF = LakeFSContext.newDF(spark, repoName, prevCommitID)

    val newTableName = randPrefix() + "_new_commit"
    val prevTableName = randPrefix() + "_prev_commit"
    newDF.createOrReplaceTempView(newTableName)
    prevDF.createOrReplaceTempView(prevTableName)

    // pin Exporter field to avoid serialization
    val dst = dstRoot

    val actionsDF = spark.sql(s"""
    SELECT
      CASE WHEN nkey is null THEN 'delete' ELSE 'copy' END as action,
      CASE WHEN nkey is null THEN pkey ELSE nkey END as key,
      CASE WHEN naddress is null THEN paddress ELSE naddress END as address,
      CASE WHEN netag is null THEN petag ELSE netag END as etag
    FROM
    (SELECT n.key as nkey, n.address as naddress, n.etag as netag,
      p.key as pkey, p.address as paddress, p.etag as petag
      FROM ${newTableName} n
      FULL OUTER JOIN ${prevTableName} p
      ON n.key = p.key
      WHERE n.etag <> p.etag OR n.etag is null or p.etag is null)
    """)

    actOnActions(ns, dst, commitID, actionsDF)
  }

  final private val successMsg = "Export completed successfully!"
  private def actOnActions(
      ns: String,
      dst: String,
      commitID: String,
      actionsDF: DataFrame
  ): Unit = {
    for (i <- 1 to filter.rounds()) {
      if (!export(i, ns, dst, commitID, actionsDF)) {
        // failed to export files, don't export next round
        return
      }
    }

    writeSummaryFile(true, commitID, successMsg)
  }

  private def writeSummaryFile(success: Boolean, commitID: String, content: String) = {
    val suffix = if (success) "SUCCESS" else "FAILURE"
    val time = DateTimeFormatter.ISO_INSTANT.format(java.time.Clock.systemUTC.instant())
    val dstPath = URLResolver.resolveURL(new URI(dstRoot), s"EXPORT_${commitID}_${time}_${suffix}")
    val dstFS = dstPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val stream = dstFS.create(dstPath)
    stream.writeChars(content)
    stream.close()
  }

  final private val prefixLen = 8
  private def randPrefix(): String = {
    val gen = Random.alphanumeric.dropWhile(_.isDigit)
    gen.take(prefixLen).mkString("")
  }
}

object Exporter {
  final val defaultParallelism = 10
}

/** Main exports lakeFS object to an object-store location.
 *  There are 3 options to run the Export main:
 *  1. Export all objects from branch 'main' on 'example-repo' repository to s3 location 's3://example-bucket/prefix/':
 *    submit example-repo s3://example-bucket/prefix/ --branch=main
 *  2. Export all objects from a commit 'c805e49bafb841a0875f49cd555b397340bbd9b8' on 'example-repo' repository to s3 location 's3://example-bucket/prefix/':
 *    submit example-repo s3://example-bucket/prefix/ --commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
 *  3. Export only the diff between branch 'main' and commit 'c805e49bafb841a0875f49cd555b397340bbd9b8'
 *     on 'example-repo' repository to s3 location 's3://example-bucket/prefix/':
 *    submit example-repo s3://example-bucket/prefix/ --branch=main --prev_commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
 */
object Main {
  def main(args: Array[String]) {
    val conf = new Conf(args)

    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    val endpoint = sc.hadoopConfiguration.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = sc.hadoopConfiguration.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = sc.hadoopConfiguration.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val connectionTimeout = sc.hadoopConfiguration.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY)
    val readTimeout = sc.hadoopConfiguration.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)

    val rawLocation = conf.rootLocation()
    val s3Prefix = "s3://"
    val rootLocation =
      if (rawLocation.startsWith(s3Prefix)) "s3a://" + rawLocation.substring(s3Prefix.length)
      else rawLocation
    val apiClient = ApiClient.get(
      APIConfigurations(endpoint, accessKey, secretKey, connectionTimeout, readTimeout)
    )
    val exporter = new Exporter(spark, apiClient, conf.repo(), rootLocation)

    if (conf.commit_id.isSupplied) {
      exporter.exportAllFromCommit(conf.commit_id())
      return
    }

    if (conf.prev_commit_id.isSupplied) {
      exporter.exportFrom(conf.branch(), conf.prev_commit_id())
      return
    }

    exporter.exportAllFromBranch(conf.branch())
  }
}

/** Conf options and arguments usage is documented in https://github.com/scallop/scallop/wiki
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val branch = opt[String](required = false)
  val commit_id = opt[String](required = false)
  val prev_commit_id = opt[String](required = false)
  val repo = trailArg[String](required = true)
  val rootLocation = trailArg[String](required = true)
  requireOne(commit_id, branch)
  conflicts(commit_id, List(prev_commit_id))
  verify()
}

class Handler(
    filter: KeyFilter,
    round: Int,
    ns: String,
    rootDst: String,
    serializedConf: SerializableWritable[Configuration],
    row: Row
) extends Callable[ExportStatus]
    with Serializable {
  def call(): ExportStatus = {
    handleRow(filter, round, ns, rootDst, serializedConf, row)
  }

  def handleRow(
      filter: KeyFilter,
      round: Int,
      ns: String,
      rootDst: String,
      serializedConf: SerializableWritable[Configuration],
      row: Row
  ): ExportStatus = {
    val action = row(0)
    val key = row(1).toString
    val address = row(2).toString

    if (filter.roundForKey(key) != round) {
      // skip this round
      return ExportStatus(key, success = true, "skipped")
    }

    val conf = serializedConf.value
    val srcPath = URLResolver.resolveURL(new URI(ns), address)
    val dstPath = URLResolver.resolveURL(new URI(rootDst), key)

    val dstFS = dstPath.getFileSystem(conf)

    action match {
      case "delete" => {
        try {
          dstFS.delete(dstPath, false)
          ExportStatus(key, success = true, "")
        } catch {
          case e: (IOException) =>
            ExportStatus(dstPath.toString,
                         success = false,
                         s"Unable to delete file ${dstPath}: ${e}"
                        )
        }
      }

      case "copy" => {
        try {
          org.apache.hadoop.fs.FileUtil.copy(srcPath.getFileSystem(conf),
                                             srcPath,
                                             dstFS,
                                             dstPath,
                                             false,
                                             conf
                                            )
          ExportStatus(key, success = true, "")
        } catch {
          case e: (FileNotFoundException) =>
            ExportStatus(
              dstPath.toString,
              success = true,
              s"Unable to copy file ${dstPath} from source ${srcPath} since source file is missing: ${e}"
            )
          case e: (IOException) =>
            ExportStatus(dstPath.toString,
                         success = false,
                         s"Unable to copy file ${dstPath} from source ${srcPath}: ${e}"
                        )
        }
      }
    }
  }
}
