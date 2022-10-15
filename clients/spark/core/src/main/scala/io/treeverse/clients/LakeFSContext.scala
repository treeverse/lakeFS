package io.treeverse.clients

import io.treeverse.lakefs.catalog.Entry
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.concurrent.TimeUnit

object LakeFSContext {
  val LAKEFS_CONF_API_URL_KEY = "lakefs.api.url"
  val LAKEFS_CONF_API_ACCESS_KEY_KEY = "lakefs.api.access_key"
  val LAKEFS_CONF_API_SECRET_KEY_KEY = "lakefs.api.secret_key"
  val LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY = "lakefs.api.connection.timeout_seconds"
  val LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY = "lakefs.api.read.timeout_seconds"
  val LAKEFS_CONF_JOB_REPO_NAME_KEY = "lakefs.job.repo_name"
  val LAKEFS_CONF_JOB_STORAGE_NAMESPACE_KEY = "lakefs.job.storage_namespace"
  val LAKEFS_CONF_JOB_COMMIT_ID_KEY = "lakefs.job.commit_id"
  val LAKEFS_CONF_GC_NUM_RANGE_PARTITIONS = "lakefs.gc.range.num_partitions"
  val LAKEFS_CONF_GC_NUM_ADDRESS_PARTITIONS = "lakefs.gc.address.num_partitions"
  val LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_ISO_DATETIME_KEY = "lakefs.debug.gc.max_commit_iso_datetime"
  val LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_EPOCH_SECONDS_KEY = "lakefs.debug.gc.max_commit_epoch_seconds"
  val LAKEFS_CONF_DEBUG_GC_REPRODUCE_RUN_ID_KEY = "lakefs.debug.gc.reproduce_run_id"
  val LAKEFS_CONF_GC_DO_MARK = "lakefs.gc.do_mark"
  val LAKEFS_CONF_GC_DO_SWEEP = "lakefs.gc.do_sweep"
  val LAKEFS_CONF_GC_MARK_ID = "lakefs.gc.mark_id"
  val LAKEFS_CONF_DEBUG_GC_NO_DELETE_KEY = "lakefs.debug.gc.no_delete"

  val MARK_ID_KEY = "mark_id"
  val RUN_ID_KEY = "run_id"
  val COMMITS_LOCATION_KEY = "commits_location"
  val DEFAULT_LAKEFS_CONF_GC_NUM_RANGE_PARTITIONS = 50
  val DEFAULT_LAKEFS_CONF_GC_NUM_ADDRESS_PARTITIONS = 200

  private def newRDD(
      sc: SparkContext,
      repoName: String,
      storageNamespace: String,
      commitID: String,
      inputFormatClass: Class[_ <: LakeFSBaseInputFormat]
  ): RDD[(Array[Byte], WithIdentifier[Entry])] = {
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.set(LAKEFS_CONF_JOB_REPO_NAME_KEY, repoName)
    conf.set(LAKEFS_CONF_JOB_COMMIT_ID_KEY, commitID)
    conf.set(LAKEFS_CONF_JOB_STORAGE_NAMESPACE_KEY, storageNamespace)
    if (StringUtils.isBlank(conf.get(LAKEFS_CONF_API_URL_KEY))) {
      throw new InvalidJobConfException(s"$LAKEFS_CONF_API_URL_KEY must not be empty")
    }
    if (StringUtils.isBlank(conf.get(LAKEFS_CONF_API_ACCESS_KEY_KEY))) {
      throw new InvalidJobConfException(s"$LAKEFS_CONF_API_ACCESS_KEY_KEY must not be empty")
    }
    if (StringUtils.isBlank(conf.get(LAKEFS_CONF_API_SECRET_KEY_KEY))) {
      throw new InvalidJobConfException(s"$LAKEFS_CONF_API_SECRET_KEY_KEY must not be empty")
    }
    sc.newAPIHadoopRDD(
      conf,
      inputFormatClass,
      classOf[Array[Byte]],
      classOf[WithIdentifier[Entry]]
    )
  }

  /** Returns all entries in all ranges of the given commit, as an RDD.
   *  If no commit is given, returns all entries in all ranges of the entire repository.
   *  The same entry may be found in multiple ranges.
   */
  def newRDD(
      sc: SparkContext,
      repoName: String,
      commitID: String = ""
  ): RDD[(Array[Byte], WithIdentifier[Entry])] = {
    val inputFormatClass =
      if (StringUtils.isNotBlank(commitID)) classOf[LakeFSCommitInputFormat]
      else classOf[LakeFSAllRangesInputFormat]

    newRDD(sc, repoName, "", commitID, inputFormatClass)
  }

  private def newDF(
      spark: SparkSession,
      repoName: String,
      storageNamespace: String,
      commitID: String,
      inputFormatClass: Class[_ <: LakeFSBaseInputFormat]
  ): DataFrame = {
    val rdd =
      newRDD(spark.sparkContext, repoName, storageNamespace, commitID, inputFormatClass).map {
        case (k, v) =>
          val entry = v.message
          Row(
            new String(k),
            entry.address,
            entry.eTag,
            new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(entry.getLastModified.seconds)),
            entry.size,
            new String(v.rangeID)
          )
      }
    val schema = new StructType()
      .add(StructField("key", StringType))
      .add(StructField("address", StringType))
      .add(StructField("etag", StringType))
      .add(StructField("last_modified", TimestampType))
      .add(StructField("size", LongType))
      .add(StructField("range_id", StringType))
    spark.createDataFrame(rdd, schema)
  }

  /** Returns all entries in all ranges found in this storage namespace.
   *  The same entry may be found in multiple ranges.
   *
   *  The storage namespace is expected to be a URI accessible by Hadoop.
   */
  def newDF(
      spark: SparkSession,
      storageNamespace: String
  ): DataFrame = {
    newDF(spark, "", storageNamespace, "", classOf[LakeFSAllRangesInputFormat])
  }

  /** Returns all entries in all ranges of the given commit, as a DataFrame.
   *  If no commit is given, returns all entries in all ranges of the entire repository.
   *  The same entry may be found in multiple ranges.
   */
  def newDF(
      spark: SparkSession,
      repoName: String,
      commitID: String = ""
  ): DataFrame = {
    val inputFormatClass =
      if (StringUtils.isNotBlank(commitID)) classOf[LakeFSCommitInputFormat]
      else classOf[LakeFSAllRangesInputFormat]
    newDF(spark, repoName, "", commitID, inputFormatClass)
  }
}
