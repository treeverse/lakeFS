package io.treeverse.gc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import io.treeverse.clients.ApiClient
import io.treeverse.clients.LakeFSContext
import io.treeverse.clients.LakeFSJobParams

class ActiveCommitsAddressLister(val apiClient: ApiClient, val repoName: String)
    extends CommittedAddressLister {

  override def listCommittedAddresses(
      spark: SparkSession,
      storageNamespace: String,
      clientStorageNamespace: String
  ): DataFrame = {

    val prepareResult = apiClient.prepareGarbageCollectionCommits(repoName, "")
    var commitsDF = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(prepareResult.getGcCommitsLocation)
    commitsDF = commitsDF.filter(commitsDF("expired") === true).select("commit_id")
    LakeFSContext.newDF(spark,
                        LakeFSJobParams.forCommits(repoName,
                                                   commitsDF.collect().map(_.getString(0)),
                                                   "experimental-unified-gc"
                                                  )
                       )
  }
}
