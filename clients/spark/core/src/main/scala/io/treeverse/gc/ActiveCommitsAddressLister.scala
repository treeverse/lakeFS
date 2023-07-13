package io.treeverse.gc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import io.treeverse.clients.ApiClient
import io.treeverse.clients.LakeFSContext
import io.treeverse.clients.LakeFSJobParams
import java.net.URI
import io.lakefs.clients.api.ApiException
import org.apache.http.HttpStatus

class ActiveCommitsAddressLister(
    val apiClient: ApiClient,
    val repoName: String,
    val storageType: String
) extends CommittedAddressLister {

  override def listCommittedAddresses(
      spark: SparkSession,
      storageNamespace: String,
      clientStorageNamespace: String
  ): DataFrame = {
    val prepareResult =
      try {
        apiClient.prepareGarbageCollectionCommits(repoName, "")
      } catch {
        case e: ApiException => {
          if (e.getCode() == HttpStatus.SC_NOT_FOUND) {
            // no garbage collection rules, consider all commits as active:
            return new NaiveCommittedAddressLister().listCommittedAddresses(spark,
                                                                            storageNamespace,
                                                                            clientStorageNamespace
                                                                           )
          } else {
            throw e
          }
        }
      }
    val gcCommitsLocation =
      ApiClient.translateURI(new URI(prepareResult.getGcCommitsLocation), storageType)
    var commitsDF = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(gcCommitsLocation.toString)
    commitsDF = commitsDF.filter(commitsDF("expired") === false).select("commit_id")
    val df = LakeFSContext.newDF(spark,
                                 LakeFSJobParams.forCommits(repoName,
                                                            commitsDF.collect().map(_.getString(0)),
                                                            "experimental-unified-gc"
                                                           )
                                )
    val normalizedClientStorageNamespace =
      if (clientStorageNamespace.endsWith("/")) clientStorageNamespace
      else clientStorageNamespace + "/"

    filterAddresses(spark, df, normalizedClientStorageNamespace)
  }
}
