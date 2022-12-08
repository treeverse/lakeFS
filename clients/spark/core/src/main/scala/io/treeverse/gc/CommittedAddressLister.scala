package io.treeverse.gc

import org.apache.spark.sql.DataFrame
import io.treeverse.clients.LakeFSContext
import org.apache.spark.sql.SparkSession
import io.treeverse.clients.LakeFSJobParams
import io.treeverse.lakefs.catalog.Entry.AddressType
import org.apache.spark.sql.functions.col

trait CommittedAddressLister {
  def listCommittedAddresses(
      spark: SparkSession,
      storageNamespace: String,
      clientStorageNamespace: String
  ): DataFrame
}

class NaiveCommittedAddressLister extends CommittedAddressLister {
  override def listCommittedAddresses(
      spark: SparkSession,
      storageNamespace: String,
      clientStorageNamespace: String
  ): DataFrame = {
    val normalizedStorageNamespace =
      if (storageNamespace.endsWith("/")) storageNamespace else storageNamespace + "/"
    val params = LakeFSJobParams.forStorageNamespace(
      normalizedStorageNamespace,
      UncommittedGarbageCollector.UNCOMMITTED_GC_SOURCE_NAME
    )
    val df = LakeFSContext.newDF(spark, params)

    val normalizedClientStorageNamespace =
      if (clientStorageNamespace.endsWith("/")) clientStorageNamespace
      else clientStorageNamespace + "/"

    import spark.implicits._
    df
      .select("address_type", "address")
      .filter(row => {
        val (addrType, addr) = (row.getString(0), row.getString(1))
        addrType.equals(AddressType.RELATIVE.name) || addr.startsWith(
          normalizedClientStorageNamespace
        )
      })
      .map(row => {
        val (addrType, addr) = (row.getString(0), row.getString(1))
        if (addrType.equals(AddressType.RELATIVE.name)) {
          (addr)
        } else {
          (addr.substring(normalizedStorageNamespace.length - 1))
        }
      })
      .distinct()
      .toDF("address")
  }
}
