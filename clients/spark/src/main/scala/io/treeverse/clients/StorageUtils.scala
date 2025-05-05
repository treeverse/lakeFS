package io.treeverse.clients

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.{
  AWSCredentials,
  AWSStaticCredentialsProvider,
  BasicAWSCredentials,
  BasicSessionCredentials
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
import java.lang.reflect.Method

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
        credentialsProvider: Option[AWSCredentialsProvider],
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String,
        bucket: String
    ): AmazonS3 = {
      require(awsS3ClientBuilder != null)
      require(bucket.nonEmpty)
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
      initializeS3Client(
        configuration,
        credentialsProvider,
        awsS3ClientBuilder,
        endpoint,
        bucketRegion
      )
    }

    /** Extract credentials from ANY provider using reflection and create an AWSCredentials object
     *  This fixes the compatibility issue with EMR 7.x by completely bypassing type-casting
     */
    private def extractCredentials(provider: Any): AWSCredentials = {
      if (provider == null) {
        throw new RuntimeException("Cannot extract credentials from null provider")
      }

      logger.info(s"Extracting credentials from provider of type: ${provider.getClass.getName}")

      // Helper function to safely extract a string value using reflection
      def safeGetString(obj: Any, methodNames: String*): String = {
        if (obj == null) return ""

        for (methodName <- methodNames) {
          try {
            val method = obj.getClass.getMethod(methodName)
            val result = method.invoke(obj)
            if (result != null) {
              return result.toString
            }
          } catch {
            case _: NoSuchMethodException => // Try next method
            case e: Exception =>
              logger.debug(
                s"Failed to invoke $methodName on ${obj.getClass.getName}: ${e.getMessage}"
              )
          }
        }
        "" // Return empty string if all methods fail
      }

      try {
        // First try using getCredentials if available
        val credentials =
          try {
            val getCredMethod = provider.getClass.getMethod("getCredentials")
            getCredMethod.invoke(provider)
          } catch {
            case e: Exception =>
              logger.debug(s"Failed to get credentials directly: ${e.getMessage}")
              provider // Fall back to treating the provider itself as credentials
          }

        // Extract credential components
        val accessKey =
          safeGetString(credentials, "getUserName", "getAccessKey", "getAWSAccessKeyId")
        val secretKey = safeGetString(credentials, "getPassword", "getSecretKey", "getAWSSecretKey")
        val token = safeGetString(credentials, "getToken", "getSessionToken")

        logger.info(
          s"Extracted credentials - has access key: ${accessKey.nonEmpty}, has secret: ${secretKey.nonEmpty}, has token: ${token.nonEmpty}"
        )

        if (accessKey.isEmpty || secretKey.isEmpty) {
          throw new RuntimeException(
            "Could not extract valid AWS credentials - missing access key or secret key"
          )
        }

        if (token.nonEmpty) {
          new BasicSessionCredentials(accessKey, secretKey, token)
        } else {
          new BasicAWSCredentials(accessKey, secretKey)
        }
      } catch {
        case e: Exception =>
          logger.error(s"Failed to extract credentials: ${e.getMessage}", e)
          throw new RuntimeException(
            s"Failed to extract credentials from ${provider.getClass.getName}: ${e.getMessage}",
            e
          )
      }
    }

    private def initializeS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String = null
    ): AmazonS3 = {
      // Configure client with endpoint/region
      val builder = awsS3ClientBuilder.withClientConfiguration(configuration)
      val builderWithEndpoint =
        if (endpoint != null)
          builder.withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(endpoint, region)
          )
        else if (region != null)
          builder.withRegion(region)
        else
          builder

      // Handle credentials
      val finalBuilder = credentialsProvider match {
        case Some(cp) =>
          logger.info(s"Processing credential provider of type: ${cp.getClass.getName}")

          try {
            // Try to get AWS credentials directly first
            val directAwsCredentials =
              try {
                logger.debug("Attempting to get credentials directly from provider")
                cp.getCredentials
              } catch {
                case e: Exception =>
                  logger.debug(s"Direct credential access failed: ${e.getMessage}")
                  null
              }

            if (directAwsCredentials != null) {
              // If we got credentials directly, use them
              logger.info("Using credentials retrieved directly from provider")
              builderWithEndpoint.withCredentials(
                new AWSStaticCredentialsProvider(directAwsCredentials)
              )
            } else {
              // If direct access failed, use reflection
              logger.info("Direct credential access failed or returned null, using reflection")
              val extractedCredentials = extractCredentials(cp)
              logger.info("Successfully extracted credentials via reflection")
              builderWithEndpoint.withCredentials(
                new AWSStaticCredentialsProvider(extractedCredentials)
              )
            }
          } catch {
            case e: Exception =>
              // Last resort - try to create a temporary client without credentials to use anonymous access
              logger.error(s"All credential extraction methods failed: ${e.getMessage}", e)
              logger.warn(
                "Creating S3 client without credentials - this will only work for public resources"
              )
              builderWithEndpoint
          }
        case None =>
          logger.info("No credential provider specified, using default")
          builderWithEndpoint
      }

      // Build the final client
      finalBuilder.build
    }

    private def getAWSS3Region(client: AmazonS3, bucket: String): String = {
      var request = new GetBucketLocationRequest(bucket)
      request = request.withSdkClientExecutionTimeout(TimeUnit.SECONDS.toMillis(1).intValue())
      val bucketRegion = client.getBucketLocation(request)
      Region.fromValue(bucketRegion).toAWSRegion().getName()
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
