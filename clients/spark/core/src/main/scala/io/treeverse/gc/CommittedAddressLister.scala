package io.treeverse.gc

import org.apache.spark.sql.DataFrame
import io.treeverse.clients.LakeFSContext
import org.apache.spark.sql.SparkSession
import io.treeverse.clients.LakeFSJobParams
import org.apache.spark.sql.functions._

trait CommittedAddressLister {
  def listCommittedAddresses(spark: SparkSession, storageNamespace: String): DataFrame
}

class NaiveCommittedAddressLister extends CommittedAddressLister {
  override def listCommittedAddresses(spark: SparkSession, storageNamespace: String): DataFrame = {
    var normalizedStorageNamespace = storageNamespace
    if (!normalizedStorageNamespace.endsWith("/")) {
      normalizedStorageNamespace = "/"
    }
    val params =
      LakeFSJobParams.forStorageNamespace(s"${normalizedStorageNamespace}", "uncommitted_gc")
    var df = LakeFSContext.newDF(spark, params)
    df = df
      .select("address")
      .withColumn("absolute_address", concat(lit(normalizedStorageNamespace), df("address")))
    df = df
      .select(df("absolute_address").as("address"))
      .distinct
    df
  }
}
