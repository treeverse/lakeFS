package io.treeverse.clients

import com.amazonaws.auth.{
  AWSCredentialsProvider,
  DefaultAWSCredentialsProviderChain,
  STSAssumeRoleSessionCredentialsProvider
}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.retry.PredefinedRetryPolicies.SDKDefaultRetryCondition
import com.amazonaws.retry.RetryUtils
import com.amazonaws.services.s3.model.{Region, GetBucketLocationRequest}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws._
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.UUID
import scala.util.Try

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
        configuration: ClientConfiguration,
        credentialsProvider: Option[
          Any
        ], // Generic type to accept both EMR 6.9.0 and 7.0.0 credential providers
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String,
        bucket: String
    ): AmazonS3 = {
      require(awsS3ClientBuilder != null)
      require(bucket.nonEmpty)

      // Check for Hadoop's assumed role configuration (common in EMR 7.0.0)
      val roleArn = System.getProperty("spark.hadoop.fs.s3a.assumed.role.arn")
      val usingAssumedRole = roleArn != null && !roleArn.isEmpty

      val client =
        initializeS3Client(configuration, credentialsProvider, awsS3ClientBuilder, endpoint)
      var bucketRegion =
        try {
          getAWSS3Region(client, bucket)
        } catch {
          case e: Throwable =>
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
      initializeS3Client(configuration,
                         credentialsProvider,
                         awsS3ClientBuilder,
                         endpoint,
                         bucketRegion
                        )
    }

    private def initializeS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[Any],
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String = null
    ): AmazonS3 = {
      val builder = awsS3ClientBuilder
        .withClientConfiguration(configuration)
      val builderWithEndpoint =
        if (endpoint != null && !endpoint.isEmpty)
          builder.withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(endpoint, region)
          )
        else if (region != null && !region.isEmpty)
          builder.withRegion(region)
        else
          builder

      // Detection for credential provider type with version-adaptive logic
      val builderWithCredentials = credentialsProvider match {
        case Some(provider) if provider.isInstanceOf[AWSCredentialsProvider] =>
          // EMR 6.9.0 path - direct SDK v1 credential provider
          logger.info("Using AWS SDK v1 credentials provider directly")
          builderWithEndpoint.withCredentials(provider.asInstanceOf[AWSCredentialsProvider])

        case Some(provider) if provider.getClass.getName.contains("hadoop.fs.s3a.auth") =>
          // EMR 7.0.0 path - Hadoop credential provider
          handleHadoopCredentials(builderWithEndpoint, provider)

        case _ =>
          // Default fallback path
          logger.info("Using DefaultAWSCredentialsProviderChain")
          builderWithEndpoint.withCredentials(new DefaultAWSCredentialsProviderChain())
      }
      builderWithCredentials.build
    }

    // Helper method for Hadoop credential handling (EMR 7.0.0 compatibility)
    private def handleHadoopCredentials(
        builder: AmazonS3ClientBuilder,
        provider: Any
    ): AmazonS3ClientBuilder = {
      // Check for assumed role configuration
      val roleArn = System.getProperty("spark.hadoop.fs.s3a.assumed.role.arn")
      if (roleArn != null && !roleArn.isEmpty) {
        // Role-based auth (use our STS provider)
        logger.info(s"Assuming role: $roleArn for S3 client")
        try {
          val sessionName = "lakefs-gc-" + UUID.randomUUID().toString
          val stsProvider =
            new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, sessionName)
              .withLongLivedCredentialsProvider(new DefaultAWSCredentialsProviderChain())
              .build()

          builder.withCredentials(stsProvider)
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to assume role $roleArn: ${e.getMessage}", e)
            logger.info("Falling back to DefaultAWSCredentialsProviderChain")
            builder.withCredentials(new DefaultAWSCredentialsProviderChain())
        }
      } else {
        // Fall back to default credential chain
        logger.info("Using DefaultAWSCredentialsProviderChain (Hadoop provider with no role)")
        builder.withCredentials(new DefaultAWSCredentialsProviderChain())
      }
    }

    private def getAWSS3Region(client: AmazonS3, bucket: String): String = {
      var request = new GetBucketLocationRequest(bucket)
      request = request.withSdkClientExecutionTimeout(TimeUnit.SECONDS.toMillis(1).intValue())
      val bucketRegion = client.getBucketLocation(request)
      Try(Region.fromValue(bucketRegion).toAWSRegion().getName()).getOrElse("")
    }
  }
}

class S3RetryDeleteObjectsCondition extends SDKDefaultRetryCondition {
  private val logger: Logger = LoggerFactory.getLogger(getClass.toString)
  private val XML_PARSE_BROKEN = "Failed to parse XML document"

  private val clock = java.time.Clock.systemDefaultZone

  override def shouldRetry(
      originalRequest: AmazonWebServiceRequest,
      exception: AmazonClientException,
      retriesAttempted: Int
  ): Boolean = {
    val now = clock.instant
    exception match {
      case ce: SdkClientException =>
        if (ce.getMessage contains XML_PARSE_BROKEN) {
          logger.info(s"Retry $originalRequest @$now: Received non-XML: $ce")
        } else if (RetryUtils.isThrottlingException(ce)) {
          logger.info(s"Retry $originalRequest @$now: Throttled: $ce")
        } else {
          logger.info(s"Retry $originalRequest @$now: Other client exception: $ce")
        }
        true
      case e => {
        logger.info(s"Do not retry $originalRequest @$now: Non-AWS exception: $e")
        super.shouldRetry(originalRequest, exception, retriesAttempted)
      }
    }
  }
}
