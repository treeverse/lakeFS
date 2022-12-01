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
    val normalizedStorageNamespace =
      if (storageNamespace.endsWith("/")) storageNamespace else storageNamespace + "/"
    val params = LakeFSJobParams.forStorageNamespace(
      normalizedStorageNamespace,
      UncommittedGarbageCollector.UNCOMMITTED_GC_SOURCE_NAME
    )
    var df = LakeFSContext.newDF(spark, params)
    df = df
      .select("address")
      .withColumn("absolute_address", concat(lit(normalizedStorageNamespace), df("address")))
    // TODO push down a filter to the input format, to filter out absolute addresses!
    df = df
      // TODO (niro): Revert substring after https://github.com/treeverse/lakeFS/issues/4699
      .select(df("absolute_address"),
              substring_index(col("absolute_address"), normalizedStorageNamespace, -1).as("address")
             )
      .distinct
    df
  }
}
