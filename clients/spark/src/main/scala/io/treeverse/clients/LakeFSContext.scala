package io.treeverse.clients

import io.treeverse.lakefs.catalog.Entry
import io.treeverse.lakefs.graveler.committed.RangeData
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util.concurrent.TimeUnit

object LakeFSJobParams {

  /** Use these parameters to list all entries for a specific commit in a repository.
   */
  def forCommit(repoName: String, commitID: String, sourceName: String = ""): LakeFSJobParams = {
    new LakeFSJobParams(repoName = repoName, commitIDs = Seq(commitID), sourceName = sourceName)
  }

  /** Use these parameters to list all entries from the given commits.
   */
  def forCommits(
      repoName: String,
      commitIDs: Iterable[String],
      sourceName: String = ""
  ): LakeFSJobParams = {
    new LakeFSJobParams(repoName = repoName, commitIDs = commitIDs, sourceName = sourceName)
  }

  /** Use these parameters to list all entries for in all ranges found in the storage namespace.
   *  The same entry may be listed multiple times if it is found in multiple ranges.
   */
  def forStorageNamespace(storageNamespace: String, sourceName: String = ""): LakeFSJobParams = {
    new LakeFSJobParams(storageNamespace = storageNamespace, sourceName = sourceName)
  }
}

/** @param repoName the repository to list entries for. Mutually exclusive with `storageNamespace`.
 *  @param storageNamespace the storage namespace to list entries from. Mutually exclusive with `repoName`.
 *  @param commitIDs the commits to list entries for. If empty, list all entries in the repository.
 *  @param sourceName a string describing the application using the client. Will be sent as part of the X-Lakefs-Client header.
 */
class LakeFSJobParams private (
    val repoName: String = "",
    val storageNamespace: String = "",
    val commitIDs: Iterable[String] = Iterable.empty,
    val sourceName: String = ""
) {
  if (StringUtils.isEmpty(repoName) == StringUtils.isEmpty(storageNamespace)) {
    throw new InvalidJobConfException("Exactly one of repoName or storageNamespace must be set")
  }
}

object LakeFSContext {
  private val logger: Logger = LoggerFactory.getLogger(getClass.toString)

  val LAKEFS_CONF_API_URL_KEY = "lakefs.api.url"
  val LAKEFS_CONF_API_ACCESS_KEY_KEY = "lakefs.api.access_key"
  val LAKEFS_CONF_API_SECRET_KEY_KEY = "lakefs.api.secret_key"
  val LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY = "lakefs.api.connection.timeout_seconds"
  val LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY = "lakefs.api.read.timeout_seconds"
  val LAKEFS_CONF_JOB_REPO_NAME_KEY = "lakefs.job.repo_name"
  val LAKEFS_CONF_JOB_STORAGE_NAMESPACE_KEY = "lakefs.job.storage_namespace"
  val LAKEFS_CONF_JOB_COMMIT_IDS_KEY = "lakefs.job.commit_ids"
  val LAKEFS_CONF_JOB_SOURCE_NAME_KEY = "lakefs.job.source_name"

  // Read parallelism.  Defaults to default parallelism.
  val LAKEFS_CONF_JOB_RANGE_READ_PARALLELISM = "lakefs.job.range_read_parallelism"

  val LAKEFS_CONF_GC_PREPARE_COMMITS_TIMEOUT_SECONDS = "lakefs.gc.prepare_commits.timeout_seconds"

  //  Objects that are written during this duration are not collected
  val LAKEFS_CONF_DEBUG_GC_UNCOMMITTED_MIN_AGE_SECONDS_KEY =
    "lakefs.debug.gc.uncommitted_min_age_seconds"
  val LAKEFS_CONF_GC_DO_MARK = "lakefs.gc.do_mark"
  val LAKEFS_CONF_GC_DO_SWEEP = "lakefs.gc.do_sweep"
  val LAKEFS_CONF_GC_MARK_ID = "lakefs.gc.mark_id"
  val LAKEFS_CONF_GC_S3_MIN_BACKOFF_SECONDS = "lakefs.gc.s3.min_backoff_secs"
  val LAKEFS_CONF_GC_S3_MAX_BACKOFF_SECONDS = "lakefs.gc.s3.max_backoff_secs"

  // By default, objects that are written in the last 24 hours are not collected
  val DEFAULT_GC_UNCOMMITTED_MIN_AGE_SECONDS: Int = 24 * 60 * 60
  val DEFAULT_LAKEFS_CONF_GC_S3_MIN_BACKOFF_SECONDS = 1
  val DEFAULT_LAKEFS_CONF_GC_S3_MAX_BACKOFF_SECONDS = 120
  val DEFAULT_LAKEFS_CONF_GC_PREPARE_COMMITS_TIMEOUT_SECONDS = 1200

  val metarangeReaderGetter: (Configuration, String, Boolean) => SSTableReader[RangeData] = SSTableReader.forMetaRange

  def newRDD(
      sc: SparkContext,
      params: LakeFSJobParams
  ): RDD[(Array[Byte], WithIdentifier[Entry])] = {
    val conf = sc.hadoopConfiguration

    conf.set(LAKEFS_CONF_JOB_REPO_NAME_KEY, params.repoName)
    conf.setStrings(LAKEFS_CONF_JOB_COMMIT_IDS_KEY, params.commitIDs.toArray: _*)

    conf.set(LAKEFS_CONF_JOB_STORAGE_NAMESPACE_KEY, params.storageNamespace)
    if (StringUtils.isBlank(conf.get(LAKEFS_CONF_API_URL_KEY))) {
      throw new InvalidJobConfException(s"$LAKEFS_CONF_API_URL_KEY must not be empty")
    }
    if (StringUtils.isBlank(conf.get(LAKEFS_CONF_API_ACCESS_KEY_KEY))) {
      throw new InvalidJobConfException(s"$LAKEFS_CONF_API_ACCESS_KEY_KEY must not be empty")
    }
    if (StringUtils.isBlank(conf.get(LAKEFS_CONF_API_SECRET_KEY_KEY))) {
      throw new InvalidJobConfException(s"$LAKEFS_CONF_API_SECRET_KEY_KEY must not be empty")
    }
    conf.set(LAKEFS_CONF_JOB_SOURCE_NAME_KEY, params.sourceName)

    val apiConf = APIConfigurations(
      conf.get(LAKEFS_CONF_API_URL_KEY),
      conf.get(LAKEFS_CONF_API_ACCESS_KEY_KEY),
      conf.get(LAKEFS_CONF_API_SECRET_KEY_KEY),
      conf.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY),
      conf.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY),
      conf.get(LAKEFS_CONF_JOB_SOURCE_NAME_KEY, "input_format")
    )
    val repoName = conf.get(LAKEFS_CONF_JOB_REPO_NAME_KEY)

    // This can go to executors.
    val serializedConf = new SerializableConfiguration(conf)

    val parallelism = conf.getInt(LAKEFS_CONF_JOB_RANGE_READ_PARALLELISM, sc.defaultParallelism)

    // ApiClient is not serializable, so create a new one for each partition on its executor.
    // (If we called X.flatMap directly, we would fetch the client from the cache for each
    // range, which is a bit too much.)

    // TODO(ariels): Unify with similar code in LakeFSInputFormat.getSplits
    val ranges = sc
      .parallelize(params.commitIDs.toSeq, parallelism)
      .mapPartitions(commits => {
        val apiClient = ApiClient.get(apiConf)
        val conf = serializedConf.value
        commits.flatMap(commitID => {
          val metaRangeURL = apiClient.getMetaRangeURL(repoName, commitID)
          if (metaRangeURL == "") {
            // a commit with no meta range is an empty commit.
            // this only happens for the first commit in the repository.
            None
          } else {
            val rangesReader = metarangeReaderGetter(conf, metaRangeURL, true)
            rangesReader
              .newIterator()
              .map(rd => new Range(new String(rd.id), rd.message.estimatedSize))
          }
        })
      })
      .distinct

    ranges.mapPartitions(ranges => {
      val apiClient = ApiClient.get(apiConf)
      val conf = serializedConf.value
      ranges.flatMap((range: Range) => {
        val path = new Path(apiClient.getRangeURL(repoName, range.id))
        val fs = path.getFileSystem(conf)
        val localFile = File.createTempFile("lakefs.", ".range")

        fs.copyToLocalFile(false, path, new Path(localFile.getAbsolutePath), true)
        val companion = Entry.messageCompanion
        // localFile owned by sstableReader which will delete it when closed.
        val sstableReader = new SSTableReader(localFile.getAbsolutePath, companion, true)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener((tc: TaskContext) => {
          try {
            sstableReader.close()
          } catch {
            case e: Exception =>
              logger.warn(s"close SSTable reader for $localFile (keep going): $e")
          }
          tc
        }))
        // TODO(ariels): Do we need to validate that this reader is good?  Assume _not_, this is
        // not InputFormat code so it should have slightly nicer error reports.
        sstableReader
          .newIterator()
          .map(entry => (entry.key, new WithIdentifier(entry.id, entry.message, range.id)))
      })
    })
  }

  /** Returns all entries in all ranges of the given commit, as an RDD.
   *  If no commit is given, returns all entries in all ranges of the entire repository.
   *  The same entry may be found in multiple ranges.
   *  @deprecated use [[LakeFSContext.newRDD(SparkContext,LakeFSJobParams)]] instead.
   */
  def newRDD(
      sc: SparkContext,
      repoName: String,
      commitID: String = ""
  ): RDD[(Array[Byte], WithIdentifier[Entry])] = {
    newRDD(sc, LakeFSJobParams.forCommit(repoName, commitID))
  }

  def newDF(
      spark: SparkSession,
      params: LakeFSJobParams
  ): DataFrame = {
    val rdd =
      newRDD(spark.sparkContext, params).map { case (k, v) =>
        val entry = v.message
        Row(
          new String(k),
          entry.address,
          entry.eTag,
          new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(entry.getLastModified.seconds)),
          entry.size,
          new String(v.rangeID),
          entry.metadata,
          entry.addressType.toString()
        )
      }
    val schema = new StructType()
      .add(StructField("key", StringType))
      .add(StructField("address", StringType))
      .add(StructField("etag", StringType))
      .add(StructField("last_modified", TimestampType))
      .add(StructField("size", LongType))
      .add(StructField("range_id", StringType))
      .add(StructField("user_metadata", MapType(StringType, StringType)))
      .add(StructField("address_type", StringType))
    spark.createDataFrame(rdd, schema)
  }

  /** Returns all entries in all ranges found in this storage namespace.
   *  The same entry may be found in multiple ranges.
   *
   *  The storage namespace is expected to be a URI accessible by Hadoop.
   *  @deprecated use [[LakeFSContext.newDF(SparkContext,LakeFSJobParams)]] instead.
   */
  def newDF(
      spark: SparkSession,
      storageNamespace: String
  ): DataFrame = {
    newDF(spark, LakeFSJobParams.forStorageNamespace(storageNamespace))
  }

  /** Returns all entries in all ranges of the given commit, as a DataFrame.
   *  If no commit is given, returns all entries in all ranges of the entire repository.
   *  The same entry may be found in multiple ranges.
   *  @deprecated use [[LakeFSContext.newDF(SparkSession,LakeFSJobParams)]] instead.
   */
  def newDF(
      spark: SparkSession,
      repoName: String,
      commitID: String = ""
  ): DataFrame = {
    newDF(spark, LakeFSJobParams.forCommit(repoName, commitID))
  }
}
