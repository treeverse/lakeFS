package io.treeverse.clients

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.RetryPolicyContext
import software.amazon.awssdk.core.retry.conditions.RetryCondition
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{HeadBucketRequest, S3Exception}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

object StorageUtils {
  val StorageTypeS3 = "s3"
  val StorageTypeAzure = "azure"

  /** Constructs object paths in a storage namespace.
   *
   *  @param keys keys to construct paths for
   *  @param storageNamespace the storage namespace to concat
   *  @param keepNsSchemeAndHost whether to keep a storage namespace of the form "s3://bucket/foo/" or remove its URI
   *                            scheme and host leaving it in the form "/foo/"
   *  @return object paths in a storage namespace
   */
  def concatKeysToStorageNamespace(
      keys: Seq[String],
      storageNamespace: String,
      keepNsSchemeAndHost: Boolean = true
  ): Seq[String] = {
    var sanitizedNS = storageNamespace
    if (!keepNsSchemeAndHost) {
      val uri = new URI(storageNamespace)
      sanitizedNS = uri.getPath
    }
    val addSuffixSlash =
      if (sanitizedNS.endsWith("/")) sanitizedNS else sanitizedNS.concat("/")
    val snPrefix =
      if (addSuffixSlash.startsWith("/")) addSuffixSlash.substring(1) else addSuffixSlash

    if (keys.isEmpty) return Seq.empty
    keys.map(x => snPrefix.concat(x))
  }

  object AzureBlob {
    val AccountAuthType =
      "fs.azure.account.auth.type.%s.dfs.core.windows.net"
    val AccountOAuthProviderType =
      "fs.azure.account.oauth.provider.type.%s.dfs.core.windows.net"
    val AccountOAuthClientId =
      "fs.azure.account.oauth2.client.id.%s.dfs.core.windows.net"
    val AccountOAuthClientSecret =
      "fs.azure.account.oauth2.client.secret.%s.dfs.core.windows.net"
    val AccountOAuthClientEndpoint =
      "fs.azure.account.oauth2.client.endpoint.%s.dfs.core.windows.net"
    val StorageAccountKeyProperty =
      "fs.azure.account.key.%s.dfs.core.windows.net"
    // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/storage.blobs.batch-readme#key-concepts
    // Note that there is no official java SDK documentation of the max batch size, therefore assuming the above.
    val AzureBlobMaxBulkSize = 256

    /** Converts storage namespace URIs of the form https://<storageAccountName>.blob.core.windows.net/<container>/<path-in-container>
     *  to storage account URL of the form https://<storageAccountName>.blob.core.windows.net and storage namespace format is
     *
     *  @param storageNsURI
     *  @return
     */
    def uriToStorageAccountUrl(storageNsURI: URI): String = {
      storageNsURI.getScheme + "://" + storageNsURI.getHost
    }

    def uriToStorageAccountName(storageNsURI: URI): String = {
      storageNsURI.getHost.split('.')(0)
    }

    // https://<storage_account>.blob.core.windows.net/<container>/<blob/path>
    def uriToContainerName(storageNsURI: URI): String = {
      storageNsURI.getPath.split('/')(1)
    }

    def getTenantId(authorityHost: URI): String = {
      authorityHost.getPath.split('/')(1)
    }
  }

  object S3 {
    val S3MaxBulkSize = 1000
    val S3NumRetries = 20
    val logger: Logger = LoggerFactory.getLogger(getClass.toString)

    def createAndValidateS3Client(
        retryPolicy: RetryPolicy,
        credentialsProvider: Option[AwsCredentialsProvider],
        endpoint: String,
        regionName: String,
        bucket: String
    ): S3Client = {
      require(bucket.nonEmpty)

      val client = initializeS3Client(retryPolicy, credentialsProvider, endpoint, regionName)

      var bucketExists = false
      try {
        val headBucketRequest = HeadBucketRequest.builder().bucket(bucket).build()
        client.headBucket(headBucketRequest)
        bucketExists = true
      } catch {
        case e: S3Exception =>
          logger.info(f"Could not fetch info for bucket $bucket", e)
      }

      if (!bucketExists && (regionName == null || regionName.isEmpty)) {
        throw new IllegalArgumentException(
          s"""Could not access bucket "$bucket" and no region was provided"""
        )
      }

      client
    }

    private def initializeS3Client(
        retryPolicy: RetryPolicy,
        credentialsProvider: Option[AwsCredentialsProvider],
        endpoint: String,
        regionName: String
    ): S3Client = {
      // Create client configuration
      val clientConfig = ClientOverrideConfiguration
        .builder()
        .retryPolicy(retryPolicy)
        .build()

      // Create S3 client builder
      val builder = S3Client
        .builder()
        .overrideConfiguration(clientConfig)

      // Configure region if provided
      val region = if (regionName != null && !regionName.isEmpty) Region.of(regionName) else null
      if (region != null) {
        builder.region(region)
      }

      // Configure endpoint if provided
      if (endpoint != null && !endpoint.isEmpty) {
        builder.endpointOverride(new URI(endpoint))
      }

      // Configure credentials if provided
      credentialsProvider.foreach(builder.credentialsProvider)

      // Build the client
      builder.build()
    }
  }
}

class S3RetryCondition extends RetryCondition {
  private val logger: Logger = LoggerFactory.getLogger(getClass.toString)
  private val XML_PARSE_BROKEN = "Failed to parse XML document"

  override def shouldRetry(context: RetryPolicyContext): Boolean = {
    val exception = context.exception()
    val originalRequest = context.originalRequest()
    val retriesAttempted = context.retriesAttempted()

    exception match {
      case s3e: S3Exception =>
        val message = s3e.getMessage
        if (message != null && message.contains(XML_PARSE_BROKEN)) {
          logger.info(s"Retry $originalRequest: Received non-XML: $s3e")
          true
        } else if (
          s3e.statusCode() == 429 ||
          (s3e.statusCode() >= 500 && s3e.statusCode() < 600)
        ) {
          logger.info(s"Retry $originalRequest: Throttled or server error: $s3e")
          true
        } else {
          logger.info(s"Retry $originalRequest: Other S3 exception: $s3e")
          true
        }
      case e: Exception => {
        logger.info(s"Do not retry $originalRequest: Non-S3 exception: $e")
        false
      }
    }
  }
}

class S3RetryDeleteObjectsCondition extends RetryCondition {
  private val logger: Logger = LoggerFactory.getLogger(getClass.toString)

  override def shouldRetry(context: RetryPolicyContext): Boolean = {
    val exception = context.exception()
    val originalRequest = context.originalRequest()

    exception match {
      case s3e: S3Exception =>
        if (s3e.statusCode() == 429 || (s3e.statusCode() >= 500 && s3e.statusCode() < 600)) {
          logger.info(s"Retry $originalRequest: Throttled or server error: $s3e")
          true
        } else {
          logger.info(s"Don't retry $originalRequest: Other S3 exception: $s3e")
          false
        }
      case e: Exception =>
        logger.info(s"Don't retry $originalRequest: Non-S3 exception: $e")
        false
    }
  }
}
