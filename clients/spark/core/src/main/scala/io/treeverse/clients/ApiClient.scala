package io.treeverse.clients

import com.google.common.cache.CacheBuilder
import io.lakefs.clients.api
import io.lakefs.clients.api.RetentionApi
import io.lakefs.clients.api.ConfigApi
import io.lakefs.clients.api.model.{
  GarbageCollectionPrepareRequest,
  GarbageCollectionPrepareResponse
}

import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.concurrent.Callable
import java.util.regex.Pattern

private object ApiClient {
  val StorageTypeS3 = "s3"
  val StorageTypeAzure = "azure"

  /** Translate the protocol of uri from "standard"-ish "s3" to "s3a", to
   *  trigger processing by S3AFileSystem.
   */
  def translateURI(uri: URI, storageType: String): URI = {
    if ((storageType == StorageTypeS3) && (uri.getScheme == "s3")) {
      return new URI("s3a",
                     uri.getUserInfo,
                     uri.getHost,
                     uri.getPort,
                     uri.getPath,
                     uri.getQuery,
                     uri.getFragment
                    )
    } else if (storageType == StorageTypeAzure) {

      /** regex to catch url of type: https://StorageAccountName.blob.core.windows.net/Container[/BlobName] and convert it to abfs url */
      val regex = "https?://([^.]+)(\\.[^/]+)(?:/([^/]+)(.+))?"
      val pattern = Pattern.compile(regex)
      val matcher = pattern.matcher(uri.toString())
      if (matcher.find()) {
        return new URI(
          s"abfs://${matcher.group(3)}@${matcher.group(1)}.dfs.core.windows.net${matcher.group(4)}"
        )
      }
      uri
    } else {
      uri
    }
  }
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
  private val configApi = new ConfigApi(client)

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

        ApiClient
          .translateURI(URI.create(repo.getStorageNamespace), getBlockstoreType())
          .normalize()
          .toString
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

  def getBlockstoreType(): String = {
    val storageConfig = configApi.getStorageConfig()
    storageConfig.getBlockstoreType()
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
