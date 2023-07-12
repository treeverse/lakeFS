package io.treeverse.gc

import io.treeverse.clients.{LakeFSContext, LakeFSJobParams}
import io.treeverse.lakefs.catalog.Entry.AddressType
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CommittedAddressLister {
  def listCommittedAddresses(
      spark: SparkSession,
      storageNamespace: String,
      clientStorageNamespace: String
  ): DataFrame

  def filterAddresses(
      spark: SparkSession,
      df: DataFrame,
      normalizedClientStorageNamespace: String
  ): DataFrame = {
    // extract the schema
    var storageScheme = new Path(normalizedClientStorageNamespace).toUri.getScheme
    if (storageScheme.isEmpty) {
      throw new IllegalArgumentException(
        s"Invalid storage namespace - missing scheme $normalizedClientStorageNamespace"
      )
    }
    storageScheme += ":"

    import spark.implicits._
    val addressesDF = df
      .select("address_type", "address")
      .filter(row => {
        val (addrType, addr) = (row.getString(0), row.getString(1))
        // allow:
        // - type relative
        // - non relative to repo storage namespace
        // - non relative with no schema (relative)
        addrType.equals(AddressType.RELATIVE.name) ||
        addr.startsWith(normalizedClientStorageNamespace) ||
        !addr.startsWith(storageScheme)
      })
      .map(row => {
        val (addrType, addr) = (row.getString(0), row.getString(1))
        // if not relative and starts with storage namespace - trim to relative
        val relativeType = addrType.equals(AddressType.RELATIVE.name)
        if (!relativeType && addr.startsWith(normalizedClientStorageNamespace)) {
          addr.substring(normalizedClientStorageNamespace.length)
        } else {
          addr
        }
      })
      .toDF("address")
    addressesDF
      .repartition(addressesDF.col("address"))
      .distinct()
  }
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

    filterAddresses(spark, df, normalizedClientStorageNamespace)
  }
}
