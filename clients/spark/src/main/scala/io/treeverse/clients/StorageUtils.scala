package io.treeverse.clients

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.{
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

      // Extract credentials early and create a new static provider
      val safeCredentialsProvider: Option[AWSCredentialsProvider] =
        credentialsProvider.flatMap(cp => {
          try {
            logger.info(s"Processing credential provider type: ${cp.getClass.getName}")

            // First attempt to directly get credentials
            try {
              val creds = cp.getCredentials
              if (creds != null) {
                logger.info("Successfully extracted credentials directly")
                Some(new AWSStaticCredentialsProvider(creds))
              } else {
                logger.warn("Credentials provider returned null credentials")
                None
              }
            } catch {
              case e: Exception =>
                // If direct method fails, try reflection
                logger.info(
                  s"Direct credential extraction failed: ${e.getMessage}, trying reflection"
                )
                extractCredentialsUsingReflection(cp)
            }
          } catch {
            case e: Exception =>
              logger.error(s"All credential extraction methods failed: ${e.getMessage}", e)
              None
          }
        })

      // Now use our safe provider for all S3 operations
      val client =
        createS3Client(configuration, safeCredentialsProvider, awsS3ClientBuilder, endpoint)

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

      // Create the final client with the right region
      createS3Client(configuration,
                     safeCredentialsProvider,
                     awsS3ClientBuilder,
                     endpoint,
                     bucketRegion
                    )
    }

    /** Extract credentials using reflection from any type of provider
     */
    private def extractCredentialsUsingReflection(provider: Any): Option[AWSCredentialsProvider] = {
      if (provider == null) {
        logger.warn("Provider is null, cannot extract credentials")
        return None
      }

      try {
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
                logger.debug(s"Failed to invoke $methodName: ${e.getMessage}")
            }
          }
          "" // Return empty string if all methods fail
        }

        // Get the credentials object using reflection
        val credentials =
          try {
            val getCredMethod = provider.getClass.getMethod("getCredentials")
            getCredMethod.invoke(provider)
          } catch {
            case e: Exception =>
              logger.debug(s"Failed to get credentials via reflection: ${e.getMessage}")
              provider // Fall back to treating the provider itself as credentials
          }

        // Extract credential components
        val accessKey =
          safeGetString(credentials, "getUserName", "getAccessKey", "getAWSAccessKeyId")
        val secretKey = safeGetString(credentials, "getPassword", "getSecretKey", "getAWSSecretKey")
        val token = safeGetString(credentials, "getToken", "getSessionToken")

        logger.info(
          s"Extracted credential components via reflection - has access key: ${accessKey.nonEmpty}, has secret: ${secretKey.nonEmpty}, has token: ${token.nonEmpty}"
        )

        if (accessKey.isEmpty || secretKey.isEmpty) {
          logger.warn("Could not extract valid credentials - missing access key or secret key")
          None
        } else {
          val awsCredentials = if (token.nonEmpty) {
            new BasicSessionCredentials(accessKey, secretKey, token)
          } else {
            new BasicAWSCredentials(accessKey, secretKey)
          }

          Some(new AWSStaticCredentialsProvider(awsCredentials))
        }
      } catch {
        case e: Exception =>
          logger.error(s"Failed to extract credentials via reflection: ${e.getMessage}", e)
          None
      }
    }

    /** Create an S3 client with the given configuration
     *  This completely replaces the old initializeS3Client method
     */
    private def createS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String = null
    ): AmazonS3 = {
      val builder = awsS3ClientBuilder.withClientConfiguration(configuration)

      // Configure endpoint or region
      val builderWithEndpoint =
        if (endpoint != null)
          builder.withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(endpoint, region)
          )
        else if (region != null)
          builder.withRegion(region)
        else
          builder

      // Add credentials if available, otherwise use default
      val finalBuilder = credentialsProvider match {
        case Some(provider) =>
          logger.info(s"Using credentials provider: ${provider.getClass.getName}")
          builderWithEndpoint.withCredentials(provider)
        case None =>
          logger.info("No credentials provider available, using default credentials chain")
          builderWithEndpoint
      }

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
