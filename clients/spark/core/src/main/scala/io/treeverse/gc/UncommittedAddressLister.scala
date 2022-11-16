package io.treeverse.gc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

trait UncommittedAddressLister {
  def listUncommittedAddresses(spark: SparkSession, repo: String): DataFrame
}

class NaiveUncommittedAddressLister extends UncommittedAddressLister {
  override def listUncommittedAddresses(spark: SparkSession, repo: String): DataFrame = {
    null
  }
}
