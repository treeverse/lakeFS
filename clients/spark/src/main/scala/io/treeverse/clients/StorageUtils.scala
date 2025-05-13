package io.treeverse.clients

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.{AmazonClientException, AmazonWebServiceRequest}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.util.UUID

object StorageUtils {
  val StorageTypeS3 = "s3"
  val StorageTypeAzure = "azure"

  /** Constructs object paths in a storage namespace.
   *
   *  @param keys keys to construct paths for
   *  @param storageNamespace the storage namespace to concat
   *  @param keepNsSchemeAndHost whether to keep a storage namespace of the form "s3://bucket/foo/" or remove its URI
   *                           scheme and host leaving it in the form "/foo/"
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

    /** Creates and validates an S3 client with support for EMR 7.0.0's credential handling
     *
     *  This method handles two key scenarios:
     *  1. When using EMR 7.0.0+ with role-based authentication (using AssumedRoleCredentialProvider)
     *  2. Standard credential provider cases from previous versions
     *
     *  For role-based auth, we detect the Hadoop property and skip bucket location checks
     *  which may fail with permission errors when using assumed roles.
     *
     *  @param configuration AWS client configuration
     *  @param credentialsProvider Optional AWS credentials provider
     *  @param builder S3 client builder
     *  @param endpoint S3 endpoint
     *  @param region AWS region
     *  @param bucket S3 bucket name
     *  @return Configured AmazonS3 client
     */
    def createAndValidateS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        builder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String,
        bucket: String
    ): AmazonS3 = {
      require(bucket.nonEmpty)

      // Check for Hadoop's assumed role configuration (EMR 7.0.0+)
      val roleArn = System.getProperty("spark.hadoop.fs.s3a.assumed.role.arn")
      val isAssumeRoleProvider = roleArn != null && !roleArn.isEmpty

      // When using assumed role, we need to:
      // 1. Skip bucket location check which may fail due to permissions
      // 2. Use the provided region without validation
      if (isAssumeRoleProvider) {
        logger.info(s"Using role ARN: $roleArn, skipping bucket location check")

        try {
          // We create an STS credentials provider that matches what EMR would create
          // in order to access S3 with the same permissions
          val sessionName = "lakefs-gc-" + UUID.randomUUID().toString
          val stsProvider =
            new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, sessionName)
              .withLongLivedCredentialsProvider(new DefaultAWSCredentialsProviderChain())
              .build()

          // Use our STS provider with the client
          return initializeS3Client(
            configuration,
            Some(stsProvider),
            builder,
            endpoint,
            region
          )
        } catch {
          case e: Exception =>
            logger.warn(
              s"Failed to create STS credential provider for role $roleArn: ${e.getMessage}"
            )
            logger.info("Falling back to provided credentials")
            // Fall back to using standard credentials
            return initializeS3Client(
              configuration,
              credentialsProvider,
              builder,
              endpoint,
              region
            )
        }
      }

      // Standard flow for non-role based auth
      val client = initializeS3Client(configuration, credentialsProvider, builder, endpoint)

      // Try to determine the correct region for the bucket
      var bucketRegion =
        try {
          val location = client.getBucketLocation(bucket)
          // Empty location means us-east-1 (US Standard)
          if (location == null || location.isEmpty) null else location
        } catch {
          case e: Exception =>
            logger.info(f"Could not fetch region for bucket $bucket: ${e.getMessage}")
            ""
        }

      // Validate we have a region to use
      if (bucketRegion == "" && region == "") {
        throw new IllegalArgumentException(
          s"""Could not fetch region for bucket "$bucket" and no region was provided"""
        )
      }

      // Use provided region as fallback
      if (bucketRegion == "") {
        bucketRegion = region
      }

      // Create the client with the correct region
      initializeS3Client(configuration, credentialsProvider, builder, endpoint, bucketRegion)
    }

    /** Initialize an S3 client with the given configuration
     *
     *  @param configuration Client configuration
     *  @param credentialsProvider Optional credentials provider
     *  @param builder S3 client builder
     *  @param endpoint S3 endpoint
     *  @param region AWS region (optional)
     *  @return Configured AmazonS3 client
     */
    private def initializeS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        builder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String = null
    ): AmazonS3 = {
      val configuredBuilder = builder.withClientConfiguration(configuration)

      // Configure endpoint and region
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

/** Retry condition for S3 delete operations
 *
 *  This handles retrying S3 operations for common transient failures:
 *  - XML parsing errors
 *  - Rate limiting (429)
 *  - Server errors (5xx)
 */
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
          // XML parsing errors are typically transient due to incomplete responses
          logger.info(s"Retry $originalRequest: Received non-XML: $s3e")
          true
        } else if (
          s3e.getStatusCode == 429 ||
          (s3e.getStatusCode >= 500 && s3e.getStatusCode < 600)
        ) {
          // Throttling (429) and server errors (5xx) are typically transient
          logger.info(s"Retry $originalRequest: Throttled or server error: $s3e")
          true
        } else {
          // Other S3 exceptions might be transient
          logger.info(s"Retry $originalRequest: Other S3 exception: $s3e")
          true
        }
      case e: Exception => {
        // Non-S3 exceptions are unlikely to be transient
        logger.info(s"Do not retry $originalRequest: Non-S3 exception: $e")
        false
      }
    }
  }
}
