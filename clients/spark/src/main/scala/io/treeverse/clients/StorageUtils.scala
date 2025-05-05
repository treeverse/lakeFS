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

    /** Create a clean credentials provider that extracts credentials from any source
     *  This handles the compatibility issue with AssumedRoleCredentialProvider in EMR 7.x
     */
    private def createCredentialsProviderWrapper(original: Any): AWSCredentialsProvider = {
      // Check if it's already the right type
      original match {
        case awsProvider: AWSCredentialsProvider =>
          logger.debug("Provider is already an AWSCredentialsProvider, using directly")
          awsProvider
        case _ =>
          logger.info(s"Creating wrapper for credential provider: ${if (original == null) "null"
          else original.getClass.getName}")

          // Create a safe wrapper provider
          new AWSCredentialsProvider {
            override def getCredentials: AWSCredentials = {
              if (original == null) {
                throw new RuntimeException("Cannot extract credentials from null provider")
              }

              try {
                // Get the credentials method using reflection
                val getCredentialsMethod: Method = original.getClass.getMethod("getCredentials")
                val credentials: Object = getCredentialsMethod.invoke(original)

                if (credentials == null) {
                  throw new RuntimeException(
                    s"Null credentials returned from provider ${original.getClass.getName}"
                  )
                }

                logger.debug(
                  s"Successfully retrieved credentials of type ${credentials.getClass.getName}"
                )

                // Extract credential components using reflection
                def safeGetString(obj: Object, methodNames: String*): String = {
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

                // Try common credential methods
                val accessKey =
                  safeGetString(credentials, "getUserName", "getAccessKey", "getAWSAccessKeyId")
                val secretKey =
                  safeGetString(credentials, "getPassword", "getSecretKey", "getAWSSecretKey")
                val token = safeGetString(credentials, "getToken", "getSessionToken")

                logger.debug(
                  s"Extracted credentials - has access key: ${accessKey.nonEmpty}, has secret: ${secretKey.nonEmpty}, has token: ${token.nonEmpty}"
                )

                if (accessKey.isEmpty || secretKey.isEmpty) {
                  throw new RuntimeException(
                    "Could not extract valid AWS credentials - missing access key or secret key"
                  )
                }

                // Create the appropriate credentials object
                if (token.nonEmpty) {
                  new BasicSessionCredentials(accessKey, secretKey, token)
                } else {
                  new BasicAWSCredentials(accessKey, secretKey)
                }
              } catch {
                case e: Exception =>
                  logger.error(s"Failed to extract credentials from provider: ${e.getMessage}", e)
                  throw new RuntimeException(s"Failed to extract credentials: ${e.getMessage}", e)
              }
            }

            override def refresh(): Unit = {
              try {
                if (original != null) {
                  original.getClass.getMethods
                    .find(_.getName == "refresh")
                    .foreach(_.invoke(original))
                }
              } catch {
                case e: Exception =>
                  logger.debug(s"Failed to refresh credentials: ${e.getMessage}")
              }
            }
          }
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
      val builderWithCredentials = credentialsProvider match {
        case Some(cp) =>
          try {
            // Try to create a static provider from direct credentials first
            logger.info(s"Creating S3 client with credential provider: ${cp.getClass.getName}")

            try {
              // If we can get credentials directly, use them with a static provider
              val creds = cp.getCredentials
              if (creds != null) {
                logger.info("Using static credentials provider with direct credentials")
                builderWithEndpoint.withCredentials(new AWSStaticCredentialsProvider(creds))
              } else {
                throw new RuntimeException("Null credentials from provider")
              }
            } catch {
              case e: Exception =>
                // If direct access fails, use our wrapper approach
                logger.info(s"Direct credential access failed: ${e.getMessage}, using wrapper")
                val wrapper = createCredentialsProviderWrapper(cp)
                builderWithEndpoint.withCredentials(wrapper)
            }
          } catch {
            case e: Exception =>
              // Fall back to original provider if all else fails
              logger.warn(s"All credential extraction approaches failed: ${e.getMessage}")
              logger.warn("Falling back to original provider, which may fail in EMR 7.x")
              builderWithEndpoint.withCredentials(cp)
          }
        case None =>
          logger.info("No credential provider specified, using default")
          builderWithEndpoint
      }

      // Build the final client
      builderWithCredentials.build
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
