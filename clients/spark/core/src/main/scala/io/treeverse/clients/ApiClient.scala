package io.treeverse.clients

import io.lakefs.clients.api
import io.lakefs.clients.api.{ConfigApi, RetentionApi}
import io.lakefs.clients.api.model.{
  GarbageCollectionPrepareRequest,
  GarbageCollectionPrepareResponse
}
import io.treeverse.clients.StorageClientType.StorageClientType
import io.treeverse.clients.StorageUtils.{StorageTypeAzure, StorageTypeS3}

import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}

import java.net.URI
import java.util.concurrent.{Callable, TimeUnit}

// The different types of storage clients the metadata client uses to access the object store.
object StorageClientType extends Enumeration {
  type StorageClientType = Value

  val SDKClient, HadoopFS = Value
}

private object ApiClient {
  val NUM_CACHED_API_CLIENTS = 30

  case class ClientKey(apiUrl: String, accessKey: String)

  // Not a LoadingCache because the client key does not include the secret.
  // Instead, use a callable get().
  val clients: Cache[ClientKey, ApiClient] = CacheBuilder
    .newBuilder()
    .maximumSize(NUM_CACHED_API_CLIENTS)
    .build()

  /** @return an ApiClient, reusing an existing one for this URL if possible.
   */
  def get(
      apiUrl: String,
      accessKey: String,
      secretKey: String,
      connectionTimeoutSec: String = "",
      readTimeoutSec: String = ""
  ): ApiClient = clients.get(
    ClientKey(apiUrl, accessKey),
    new Callable[ApiClient] {
      def call() = new ApiClient(apiUrl, accessKey, secretKey, connectionTimeoutSec, readTimeoutSec)
    }
  )

  /** Translate uri according to two cases:
   *  If the storage type is s3 then translate the protocol of uri from "standard"-ish "s3" to "s3a", to
   *  trigger processing by S3AFileSystem.
   *  If the storage type is azure then translate the uri to abfs schema to trigger processing by AzureBlobFileSystem.
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

      /** get the host and path from url of type: https://StorageAccountName.blob.core.windows.net/Container[/BlobName],
       *  extract the storage account, container and blob path, and use them in abfs url
       */
      val storageAccountName = StorageUtils.AzureBlob.uriToStorageAccountName(uri)
      val Array(_, container, blobPath) = uri.getPath.split("/", 3)
      return new URI(
        s"abfs://${container}@${storageAccountName}.dfs.core.windows.net/${blobPath}"
      )
    } else {
      uri
    }
  }
}

// Only cached instances of ApiClient can be constructed.  The actual
// constructor is private.
class ApiClient private (
    apiUrl: String,
    accessKey: String,
    secretKey: String,
    connectionTimeoutSec: String = "",
    readTimeoutSec: String = ""
) {
  val FROM_SEC_TO_MILLISEC = 1000

  val client = new api.ApiClient
  client.setUsername(accessKey)
  client.setPassword(secretKey)
  client.setBasePath(apiUrl.stripSuffix("/"))
  if (connectionTimeoutSec != null && !connectionTimeoutSec.isEmpty) {
    val connectionTimeoutMillisec = connectionTimeoutSec.toInt * FROM_SEC_TO_MILLISEC
    client.setConnectTimeout(connectionTimeoutMillisec)
  }

  if (readTimeoutSec != null && !readTimeoutSec.isEmpty) {
    val readTimeoutMillisec = readTimeoutSec.toInt * FROM_SEC_TO_MILLISEC
    client.setReadTimeout(readTimeoutMillisec)
  }

  private val repositoriesApi = new api.RepositoriesApi(client)
  private val commitsApi = new api.CommitsApi(client)
  private val metadataApi = new api.MetadataApi(client)
  private val branchesApi = new api.BranchesApi(client)
  private val retentionApi = new RetentionApi(client)
  private val configApi = new ConfigApi(client)

  private val storageNamespaceCache: LoadingCache[StorageNamespaceCacheKey, String] =
    CacheBuilder
      .newBuilder()
      .expireAfterWrite(2, TimeUnit.MINUTES)
      .build(new CacheLoader[StorageNamespaceCacheKey, String]() {
        def load(key: StorageNamespaceCacheKey): String = keyToStorageNamespace(key)
      })

  def keyToStorageNamespace(key: StorageNamespaceCacheKey): String = {
    val repo = repositoriesApi.getRepository(key.repoName)

    val storageNamespace = key.storageClientType match {
      case StorageClientType.HadoopFS =>
        ApiClient
          .translateURI(URI.create(repo.getStorageNamespace), getBlockstoreType())
          .normalize()
          .toString
      case StorageClientType.SDKClient => repo.getStorageNamespace
      case _                           => throw new IllegalArgumentException
    }
    storageNamespace
  }

  def getStorageNamespace(repoName: String, storageClientType: StorageClientType): String = {
    storageNamespaceCache.get(StorageNamespaceCacheKey(repoName, storageClientType))
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
      URI
        .create(getStorageNamespace(repoName, StorageClientType.HadoopFS) + "/")
        .resolve(location)
        .normalize()
        .toString
    } else ""
  }

  /** Query lakeFS for a URL to the range of rangeID of repoName and
   *  translate that URL to use an appropriate Hadoop FileSystem.
   */
  def getRangeURL(repoName: String, rangeID: String): String = {
    val range = metadataApi.getRange(repoName, rangeID)
    val location = range.getLocation
    URI
      .create(getStorageNamespace(repoName, StorageClientType.HadoopFS) + "/" + location)
      .normalize()
      .toString
  }

  def getBranchHEADCommit(repoName: String, branch: String): String =
    branchesApi.getBranch(repoName, branch).getCommitId

  // Instances of case classes are compared by structure and not by reference https://docs.scala-lang.org/tour/case-classes.html.
  case class StorageNamespaceCacheKey(
      repoName: String,
      storageClientType: StorageClientType
  )
}
