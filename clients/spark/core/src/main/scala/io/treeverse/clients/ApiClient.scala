package io.treeverse.clients

import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}
import dev.failsafe.function.CheckedSupplier
import dev.failsafe.{Failsafe, FailsafeException, FailsafeExecutor, Policy, RetryPolicy}
import io.lakefs.clients.api
import io.lakefs.clients.api.model._
import io.lakefs.clients.api.{ConfigApi, RetentionApi}
import io.treeverse.clients.ApiClient.TIMEOUT_NOT_SET
import io.treeverse.clients.StorageClientType.StorageClientType
import io.treeverse.clients.StorageUtils.{StorageTypeAzure, StorageTypeS3}

import java.net.URI
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.{Callable, TimeUnit}
import scala.collection.JavaConverters._

// The different types of storage clients the metadata client uses to access the object store.
object StorageClientType extends Enumeration {
  type StorageClientType = Value

  val SDKClient, HadoopFS = Value
}

private object ApiClient {
  val NUM_CACHED_API_CLIENTS = 30
  val TIMEOUT_NOT_SET = -1

  case class ClientKey(apiUrl: String, accessKey: String)

  // Not a LoadingCache because the client key does not include the secret.
  // Instead, use a callable get().
  val clients: Cache[ClientKey, ApiClient] = CacheBuilder
    .newBuilder()
    .maximumSize(NUM_CACHED_API_CLIENTS)
    .build()

  /** @return an ApiClient, reusing an existing one for this URL if possible.
   */
  def get(conf: APIConfigurations): ApiClient = clients.get(
    ClientKey(conf.apiUrl, conf.accessKey),
    new Callable[ApiClient] {
      def call() = new ApiClient(
        APIConfigurations(conf.apiUrl,
                          conf.accessKey,
                          conf.secretKey,
                          conf.connectionTimeoutSec,
                          conf.readTimeoutSec
                         )
      )
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
      val (container: String, blobPath: String) = uri.getPath.split("/", 3) match {
        case Array(a, b, c) => (b, c)
        case Array(a, b)    => (b, "")
        case _ =>
          throw new IllegalArgumentException(
            s"Expected https://host/container or https://host/container/path in ${uri.toString}"
          )
      }
      return new URI(
        s"abfs://${container}@${storageAccountName}.dfs.core.windows.net/${blobPath}"
      )
    } else {
      uri
    }
  }
}

case class APIConfigurations(
    apiUrl: String,
    accessKey: String,
    secretKey: String,
    connectionTimeoutSec: String = "",
    readTimeoutSec: String = ""
) {
  val FROM_SEC_TO_MILLISEC = 1000

  val connectionTimeoutMillisec: Int = stringAsMillisec(connectionTimeoutSec)
  val readTimeoutMillisec: Int = stringAsMillisec(readTimeoutSec)

  def stringAsMillisec(s: String): Int = {
    if (s != null && !s.isEmpty)
      s.toInt * FROM_SEC_TO_MILLISEC
    else
      TIMEOUT_NOT_SET
  }
}

// Only cached instances of ApiClient can be constructed.  The actual
// constructor is private.
class ApiClient private (conf: APIConfigurations) {

  val client = new api.ApiClient
  client.setUsername(conf.accessKey)
  client.setPassword(conf.secretKey)
  client.setBasePath(conf.apiUrl.stripSuffix("/"))
  if (TIMEOUT_NOT_SET != conf.connectionTimeoutMillisec) {
    client.setConnectTimeout(conf.connectionTimeoutMillisec)
  }
  if (TIMEOUT_NOT_SET != conf.readTimeoutMillisec) {
    client.setReadTimeout(conf.readTimeoutMillisec)
  }

  private val repositoriesApi = new api.RepositoriesApi(client)
  private val commitsApi = new api.CommitsApi(client)
  private val metadataApi = new api.MetadataApi(client)
  private val branchesApi = new api.BranchesApi(client)
  private val retentionApi = new RetentionApi(client)
  private val configApi = new ConfigApi(client)

  private val retryWrapper = new RequestRetryWrapper(client.getReadTimeout)

  private val storageNamespaceCache: LoadingCache[StorageNamespaceCacheKey, String] =
    CacheBuilder
      .newBuilder()
      .expireAfterWrite(2, TimeUnit.MINUTES)
      .build(new CacheLoader[StorageNamespaceCacheKey, String]() {
        def load(key: StorageNamespaceCacheKey): String = keyToStorageNamespace(key)
      })

  def keyToStorageNamespace(key: StorageNamespaceCacheKey): String = {

    val getRepo = new dev.failsafe.function.CheckedSupplier[Repository]() {
      def get(): Repository = repositoriesApi.getRepository(key.repoName)
    }
    val repo = retryWrapper.wrapWithRetry(getRepo)
    val storageNamespace = key.storageClientType match {
      case StorageClientType.HadoopFS =>
        ApiClient
          .translateURI(URI.create(repo.getStorageNamespace), getBlockstoreType())
          .normalize()
          .toString
      case StorageClientType.SDKClient => repo.getStorageNamespace
      case _ => throw new IllegalArgumentException("Unknown storage type ${key.storageClientType}")
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
    val prepareGcCommits =
      new dev.failsafe.function.CheckedSupplier[GarbageCollectionPrepareResponse]() {
        def get(): GarbageCollectionPrepareResponse = retentionApi.prepareGarbageCollectionCommits(
          repoName,
          new GarbageCollectionPrepareRequest().previousRunId(previousRunID)
        )
      }
    retryWrapper.wrapWithRetry(prepareGcCommits)
  }

  def getGarbageCollectionRules(repoName: String): String = {
    val getGcRules = new dev.failsafe.function.CheckedSupplier[GarbageCollectionRules]() {
      def get(): GarbageCollectionRules = retentionApi.getGarbageCollectionRules(repoName)
    }
    val gcRules = retryWrapper.wrapWithRetry(getGcRules)
    gcRules.toString()
  }

  def getBlockstoreType(): String = {
    val getStorageConfig = new dev.failsafe.function.CheckedSupplier[StorageConfig]() {
      def get(): StorageConfig = configApi.getStorageConfig()
    }
    val storageConfig = retryWrapper.wrapWithRetry(getStorageConfig)
    storageConfig.getBlockstoreType()
  }

  def getCommit(repoName: String, commitID: String): api.model.Commit = {
    val getCommit = new dev.failsafe.function.CheckedSupplier[Commit]() {
      def get(): Commit = commitsApi.getCommit(repoName, commitID)
    }
    retryWrapper.wrapWithRetry(getCommit)
  }

  def getMetaRangeURL(repoName: String, commit: api.model.Commit): String = {
    val metaRangeID = commit.getMetaRangeId
    if (metaRangeID != "") {
      val getMetaRange = new dev.failsafe.function.CheckedSupplier[StorageURI]() {
        def get(): StorageURI = metadataApi.getMetaRange(repoName, metaRangeID)
      }
      val metaRange = retryWrapper.wrapWithRetry(getMetaRange)
      val location = metaRange.getLocation
      URI
        .create(getStorageNamespace(repoName, StorageClientType.HadoopFS) + "/")
        .resolve(location)
        .normalize()
        .toString
    } else ""
  }

  /** Query lakeFS for a URL to the metarange of commitID of repoName and
   *  translate that URL to use an appropriate Hadoop FileSystem.
   */
  def getMetaRangeURL(repoName: String, commitID: String): String = {
    val commit = getCommit(repoName, commitID)
    getMetaRangeURL(repoName, commit)
  }

  /** Query lakeFS for a URL to the range of rangeID of repoName and
   *  translate that URL to use an appropriate Hadoop FileSystem.
   */
  def getRangeURL(repoName: String, rangeID: String): String = {
    val getRange = new dev.failsafe.function.CheckedSupplier[StorageURI]() {
      def get(): StorageURI = metadataApi.getRange(repoName, rangeID)
    }
    val range = retryWrapper.wrapWithRetry(getRange)
    val location = range.getLocation
    URI
      .create(getStorageNamespace(repoName, StorageClientType.HadoopFS) + "/" + location)
      .normalize()
      .toString
  }

  def getBranchHEADCommit(repoName: String, branch: String): String = {
    val getBranch = new dev.failsafe.function.CheckedSupplier[Ref]() {
      def get(): Ref = branchesApi.getBranch(repoName, branch)
    }
    val response = retryWrapper.wrapWithRetry(getBranch)
    response.getCommitId
  }

  // Instances of case classes are compared by structure and not by reference https://docs.scala-lang.org/tour/case-classes.html.
  case class StorageNamespaceCacheKey(
      repoName: String,
      storageClientType: StorageClientType
  )
}

class RequestRetryWrapper(
    val readTimeout: Int,
    val maxDurationSeconds: Double = -1,
    val maxNumRetries: Int = 5
) {
  val UnsetMaxDuration = -1

  var maxDuration = maxDurationSeconds
  if (maxDuration == UnsetMaxDuration) {
    maxDuration = maxNumRetries * readTimeout + 7.1 * 1
  }

  private val retryPolicy: Policy[Any] = RetryPolicy
    .builder()
    .withBackoff(1, 20, ChronoUnit.SECONDS)
    .withJitter(.25)
    .withMaxRetries(maxNumRetries)
    .withMaxDuration(Duration.ofMillis((1000 * maxDuration).asInstanceOf[Long]))
    .build()

  // https://failsafe.dev/faqs/#how-does-failsafe-use-threads failsafe uses a thread pool for retries concurrency
  private val failSafeExecutor: FailsafeExecutor[Any] = Failsafe.`with`((Seq(retryPolicy).asJava))

  def wrapWithRetry[T](fn: CheckedSupplier[T]): T = {
    try {
      failSafeExecutor.get(fn)
    } catch {
      case e: (FailsafeException) =>
        throw e.getCause
      case e: Exception => throw e
    }
  }
}
