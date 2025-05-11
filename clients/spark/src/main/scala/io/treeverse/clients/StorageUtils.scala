package io.treeverse.clients

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.retry.{PredefinedRetryPolicies, RetryPolicy}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{HeadBucketRequest, S3Exception}
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
    // ... Azure code unchanged ...
  }

  object S3 {
    val S3MaxBulkSize = 1000
    val S3NumRetries = 20
    val logger: Logger = LoggerFactory.getLogger(getClass.toString)

    def createAndValidateS3Client(
        retryPolicy: RetryPolicy,
        credentialsProvider: Option[AWSCredentialsProvider],
        endpoint: String,
        regionName: String,
        bucket: String,
        pathStyleAccess: Boolean = false // Added the missing parameter
    ): AmazonS3 = {
      require(bucket.nonEmpty)

      val client =
        initializeS3Client(retryPolicy, credentialsProvider, endpoint, regionName, pathStyleAccess)

      var bucketExists = false
      try {
        client.headBucket(new HeadBucketRequest(bucket))
        bucketExists = true
      } catch {
        case e: Exception =>
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
        credentialsProvider: Option[AWSCredentialsProvider],
        endpoint: String,
        regionName: String,
        pathStyleAccess: Boolean
    ): AmazonS3 = {
      // Create client configuration
      val clientConfig = new ClientConfiguration()
        .withRetryPolicy(retryPolicy)

      // Create S3 client builder
      val builder = AmazonS3ClientBuilder
        .standard()
        .withClientConfiguration(clientConfig)
        .withPathStyleAccessEnabled(pathStyleAccess)

      // Configure region if provided
      if (regionName != null && !regionName.isEmpty) {
        builder.withRegion(regionName)
      }

      // Configure endpoint if provided
      if (endpoint != null && !endpoint.isEmpty) {
        builder.withEndpointConfiguration(
          new AmazonS3ClientBuilder.EndpointConfiguration(endpoint, regionName)
        )
      }

      // Configure credentials if provided
      credentialsProvider.foreach(builder.withCredentials)

      // Build the client
      builder.build()
    }
  }
}

// Using v1 RetryPolicy.RetryCondition
class S3RetryCondition extends RetryPolicy.RetryCondition {
  private val logger: Logger = LoggerFactory.getLogger(getClass.toString)
  private val XML_PARSE_BROKEN = "Failed to parse XML document"

  override def shouldRetry(
      originalRequest: AmazonWebServiceRequest,
      exception: AmazonClientException,
      retriesAttempted: Int
  ): Boolean = {
    exception match {
      case s3e: AmazonS3Exception =>
        val message = s3e.getMessage
        if (message != null && message.contains(XML_PARSE_BROKEN)) {
          logger.info(s"Retry $originalRequest: Received non-XML: $s3e")
          true
        } else if (
          s3e.getStatusCode == 429 ||
          (s3e.getStatusCode >= 500 && s3e.getStatusCode < 600)
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

class S3RetryDeleteObjectsCondition extends RetryPolicy.RetryCondition {
  private val logger: Logger = LoggerFactory.getLogger(getClass.toString)

  override def shouldRetry(
      originalRequest: AmazonWebServiceRequest,
      exception: AmazonClientException,
      retriesAttempted: Int
  ): Boolean = {
    exception match {
      case s3e: AmazonS3Exception =>
        if (s3e.getStatusCode == 429 || (s3e.getStatusCode >= 500 && s3e.getStatusCode < 600)) {
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
