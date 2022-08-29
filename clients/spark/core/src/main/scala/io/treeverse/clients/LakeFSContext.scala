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
  val LAKEFS_CONF_JOB_COMMIT_ID_KEY = "lakefs.job.commit_id"

  def newRDD(
      sc: SparkContext,
      repoName: String,
      commitID: String
  ): RDD[(Array[Byte], WithIdentifier[Entry])] = {
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.set(LAKEFS_CONF_JOB_REPO_NAME_KEY, repoName)
    conf.set(LAKEFS_CONF_JOB_COMMIT_ID_KEY, commitID)
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
      classOf[LakeFSInputFormat],
      classOf[Array[Byte]],
      classOf[WithIdentifier[Entry]]
    )
  }

  def newDF(spark: SparkSession, repoName: String, commitID: String): DataFrame = {
    val rdd = newRDD(spark.sparkContext, repoName, commitID).map { case (k, v) =>
      val entry = v.message
      Row(
        new String(k),
        entry.address,
        entry.eTag,
        new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(entry.getLastModified.seconds)),
        entry.size
      )
    }
    val schema = new StructType()
      .add(StructField("key", StringType))
      .add(StructField("address", StringType))
      .add(StructField("etag", StringType))
      .add(StructField("last_modified", TimestampType))
      .add(StructField("size", LongType))
    spark.createDataFrame(rdd, schema)
  }
}
