package io.treeverse.clients

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.AmazonClientException
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
    val AzureBlobMaxBulkSize = 256

    /** Converts storage namespace URIs of the form https://<storageAccountName>.blob.core.windows.net/<container>/<path-in-container>
     *  to storage account URL of the form https://<storageAccountName>.blob.core.windows.net
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
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        builder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String,
        bucket: String
    ): AmazonS3 = {
      require(bucket.nonEmpty)

      // ONLY FOR EMR 7.0.0: Check for Hadoop's assumed role configuration
      val emr7AssumedRole = Option(System.getProperty("fs.s3a.assumed.role.arn"))
        .orElse(Option(System.getProperty("spark.hadoop.fs.s3a.assumed.role.arn")))

      // Skip bucket location check only if running on EMR 7.0.0 with assumed role
      if (emr7AssumedRole.isDefined) {
        logger.info(s"EMR 7.0.0 detected with assumed role: ${emr7AssumedRole.get}")
        logger.info("Skipping bucket location check to avoid credential provider issues")
        return initializeS3Client(configuration, credentialsProvider, builder, endpoint, region)
      }

      // For all other cases (including EMR 6.9.0) - use the original flow unchanged
      val client = initializeS3Client(configuration, credentialsProvider, builder, endpoint)

      var bucketRegion =
        try {
          val location = client.getBucketLocation(bucket)
          if (location == null || location.isEmpty) null else location
        } catch {
          case e: Exception =>
            logger.info(f"Could not fetch region for bucket $bucket", e)
            ""
        }
      if (bucketRegion == "" && region == "") {
        throw new IllegalArgumentException(
          s"""Could not fetch region for bucket "$bucket" and no region was provided"""
        )
      }
      if (bucketRegion == "") {
        bucketRegion = region
      }
      initializeS3Client(configuration, credentialsProvider, builder, endpoint, bucketRegion)
    }

    private def initializeS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        builder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String = null
    ): AmazonS3 = {
      val configuredBuilder = builder.withClientConfiguration(configuration)

      if (endpoint != null && !endpoint.isEmpty) {
        configuredBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(endpoint, region)
        )
      } else if (region != null && !region.isEmpty) {
        configuredBuilder.withRegion(region)
      }

      // Apply credentials if provided
      credentialsProvider.foreach(configuredBuilder.withCredentials)

      configuredBuilder.build()
    }
  }
}

class S3RetryDeleteObjectsCondition extends RetryPolicy.RetryCondition {
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
