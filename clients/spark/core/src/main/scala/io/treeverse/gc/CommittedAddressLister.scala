package io.treeverse.gc

import org.apache.spark.sql.DataFrame
import io.treeverse.clients.LakeFSContext
import org.apache.spark.sql.SparkSession
import io.treeverse.clients.LakeFSJobParams
import io.treeverse.lakefs.catalog.Entry.AddressType
import org.apache.spark.sql.functions.col

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
      LakeFSJobParams.forStorageNamespace(s"$normalizedStorageNamespace",
                                          UncommittedGarbageCollector.UNCOMMITTED_GC_SOURCE_NAME
                                         )
    var df = LakeFSContext.newDF(spark, params)
    df = df
      // TODO (optional): push down a filter to the input format, to filter out absolute addresses!
      .filter(col("address_type") === AddressType.RELATIVE.name)
      .select("address")
      .distinct
    df
  }
}
