package io.treeverse.gc

import io.lakefs.clients.api.model.PrepareGCUncommittedResponse
import io.treeverse.clients.ApiClient
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

trait UncommittedAddressLister {
  def listUncommittedAddresses(spark: SparkSession, repo: String): UncommittedGCRunInfo
}

class UncommittedGCRunInfo(val uncommittedLocation: String, val runID: String)

class DummyUncommittedAddressLister(parquetLocation: String) extends UncommittedAddressLister {
  override def listUncommittedAddresses(spark: SparkSession, repo: String): UncommittedGCRunInfo = {
    new UncommittedGCRunInfo(parquetLocation, "dummy_run_id")
  }
}

class APIUncommittedAddressLister(apiClient: ApiClient) extends UncommittedAddressLister {
  override def listUncommittedAddresses(spark: SparkSession, repo: String): UncommittedGCRunInfo = {
    var resp: PrepareGCUncommittedResponse = null
    var continuationToken: String = null
    do {
      resp = apiClient.prepareGarbageCollectionUncommitted(repo, continuationToken)
      continuationToken = resp.getContinuationToken
    } while (StringUtils.isNotBlank(continuationToken))
    val runID = resp.getRunId
    val uncommittedLocation = resp.getGcUncommittedLocation
    new UncommittedGCRunInfo(uncommittedLocation, runID)
  }
}
