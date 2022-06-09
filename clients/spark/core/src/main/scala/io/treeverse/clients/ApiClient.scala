package io.treeverse.clients

import com.google.common.cache.CacheBuilder
import io.lakefs.clients.api
import io.lakefs.clients.api.RetentionApi
import io.lakefs.clients.api.model.{
  GarbageCollectionPrepareRequest,
  GarbageCollectionPrepareResponse
}

import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.concurrent.Callable

private object ApiClient {

  /** Translate the protocol of uri from "standard"-ish "s3" to "s3a", to
   *  trigger processing by S3AFileSystem.
   */
  def translateS3(uri: URI): URI =
    if (uri.getScheme == "s3")
      new URI("s3a",
              uri.getUserInfo,
              uri.getHost,
              uri.getPort,
              uri.getPath,
              uri.getQuery,
              uri.getFragment
             )
    else
      uri
}

class ApiClient(apiUrl: String, accessKey: String, secretKey: String) {
  private val client = new api.ApiClient
  client.setUsername(accessKey)
  client.setPassword(secretKey)
  client.setBasePath(apiUrl.stripSuffix("/"))
  private val repositoriesApi = new api.RepositoriesApi(client)
  private val commitsApi = new api.CommitsApi(client)
  private val metadataApi = new api.MetadataApi(client)
  private val branchesApi = new api.BranchesApi(client)
  private val retentionApi = new RetentionApi(client)

  private val storageNamespaceCache =
    CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.MINUTES).build[String, String]()

  private class CallableFn(val fn: () => String) extends Callable[String] {
    def call(): String = fn()
  }

  def getStorageNamespace(repoName: String): String = {
    storageNamespaceCache.get(
      repoName,
      new CallableFn(() => {
        val repo = repositoriesApi.getRepository(repoName)

        ApiClient.translateS3(URI.create(repo.getStorageNamespace)).normalize().toString
      })
    )
  }

  def prepareGarbageCollectionCommits(
      repoName: String,
      previousRunID: String
  ): GarbageCollectionPrepareResponse = {
    retentionApi.prepareGarbageCollectionCommits(
      repoName,
      new GarbageCollectionPrepareRequest().previousRunId(previousRunID)
    )
  }

  def getGarbageCollectionRules(repoName: String): String = {
    val gcRules = retentionApi.getGarbageCollectionRules(repoName)
    gcRules.toString()
  }

  /** Query lakeFS for a URL to the metarange of commitID of repoName and
   *  translate that URL to use an appropriate Hadoop FileSystem.
   */
  def getMetaRangeURL(repoName: String, commitID: String): String = {
    val commit = commitsApi.getCommit(repoName, commitID)
    val metaRangeID = commit.getMetaRangeId
    if (metaRangeID != "") {
      val metaRange = metadataApi.getMetaRange(repoName, metaRangeID)
      val location = metaRange.getLocation
      URI.create(getStorageNamespace(repoName) + "/").resolve(location).normalize().toString
    } else ""
  }

  /** Query lakeFS for a URL to the range of rangeID of repoName and
   *  translate that URL to use an appropriate Hadoop FileSystem.
   */
  def getRangeURL(repoName: String, rangeID: String): String = {
    val range = metadataApi.getRange(repoName, rangeID)
    val location = range.getLocation
    URI.create(getStorageNamespace(repoName) + "/" + location).normalize().toString
  }

  def getBranchHEADCommit(repoName: String, branch: String): String =
    branchesApi.getBranch(repoName, branch).getCommitId
}
