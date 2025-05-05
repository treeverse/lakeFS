package io.treeverse.clients

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials, BasicSessionCredentials}
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

    /** Adapts a Hadoop AssumedRoleCredentialProvider to an AWSCredentialsProvider
     *  This fixes the compatibility issue with EMR 7.x
     */
    private def adaptAssumedRoleCredentialProvider(provider: Any): AWSCredentialsProvider = {
      provider match {
        case awsProvider: AWSCredentialsProvider =>
          // If it's already an AWSCredentialsProvider, return it directly
          awsProvider
        case assumedRoleProvider
            if assumedRoleProvider != null &&
              assumedRoleProvider.getClass.getName.endsWith("AssumedRoleCredentialProvider") =>
          // Create a more robust adapter for AssumedRoleCredentialProvider
          new AWSCredentialsProvider {
            override def getCredentials: AWSCredentials = {
              try {
                // Get the credentials from the provider
                val getCredentialsMethod = assumedRoleProvider.getClass.getMethod("getCredentials")
                val credentials = getCredentialsMethod.invoke(assumedRoleProvider)

                if (credentials == null) {
                  throw new RuntimeException("Failed to obtain credentials from provider")
                }

                // Create a simpler way to access the credential fields
                val accessMethod = (name: String) => {
                  try {
                    val method = credentials.getClass.getMethod(name)
                    val result = method.invoke(credentials)
                    if (result != null) Some(result.toString) else None
                  } catch {
                    case _: Exception => None
                  }
                }

                // Extract credential components safely
                val accessKey = accessMethod("getUserName").getOrElse(
                  accessMethod("getAccessKey").getOrElse(
                    accessMethod("getAWSAccessKeyId").getOrElse("")
                  )
                )

                val secretKey = accessMethod("getPassword").getOrElse(
                  accessMethod("getSecretKey").getOrElse(
                    accessMethod("getAWSSecretKey").getOrElse("")
                  )
                )

                val token =
                  accessMethod("getToken").getOrElse(accessMethod("getSessionToken").getOrElse(""))

                if (token.nonEmpty) {
                  new BasicSessionCredentials(accessKey, secretKey, token)
                } else {
                  new BasicAWSCredentials(accessKey, secretKey)
                }
              } catch {
                case e: Exception =>
                  logger.error(s"Failed to adapt AssumedRoleCredentialProvider: ${e.getMessage}", e)
                  throw new RuntimeException(
                    s"Failed to adapt credential provider: ${e.getMessage}",
                    e
                  )
              }
            }

            override def refresh(): Unit = {
              try {
                assumedRoleProvider.getClass.getMethods
                  .find(_.getName == "refresh")
                  .foreach(_.invoke(assumedRoleProvider))
              } catch {
                case e: Exception =>
                  logger.debug(s"Failed to refresh credentials: ${e.getMessage}")
              }
            }
          }
        case other =>
          if (other == null) {
            throw new IllegalArgumentException("Credential provider is null")
          } else {
            logger.warn(s"Unknown credential provider type: ${other.getClass.getName}")
            throw new IllegalArgumentException(
              s"Unsupported credential provider type: ${other.getClass.getName}"
            )
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
      val builder = awsS3ClientBuilder
        .withClientConfiguration(configuration)

      val builderWithEndpoint =
        if (endpoint != null)
          builder.withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(endpoint, region)
          )
        else if (region != null)
          builder.withRegion(region)
        else
          builder

      val builderWithCredentials = credentialsProvider match {
        case Some(cp) =>
          logger.info(s"Configuring S3 client with credential provider: ${cp.getClass.getName}")

          // First try with direct credentials if available
          try {
            val creds = cp.getCredentials
            if (creds != null) {
              logger.info("Using direct AWSCredentials from provider")
              val staticProvider = new AWSStaticCredentialsProvider(creds)
              return builderWithEndpoint.withCredentials(staticProvider).build
            }
          } catch {
            case e: Exception =>
              logger.info(s"Could not get direct credentials: ${e.getMessage}")
          }

          // Try with our adapter approach
          try {
            logger.info("Attempting to adapt credential provider")
            val adaptedProvider = adaptAssumedRoleCredentialProvider(cp)
            builderWithEndpoint.withCredentials(adaptedProvider)
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to adapt credential provider: ${e.getMessage}", e)
              logger.warn("Falling back to original provider")
              builderWithEndpoint.withCredentials(cp)
          }
        case None =>
          logger.info("No credential provider specified, using default")
          builderWithEndpoint
      }

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
