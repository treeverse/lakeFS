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
import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
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
            if assumedRoleProvider.getClass.getSimpleName == "AssumedRoleCredentialProvider" =>
          // If it's an AssumedRoleCredentialProvider, create an adapter
          new AWSCredentialsProvider {
            override def getCredentials: AWSCredentials = {
              // Use reflection to safely get credentials without direct casting
              try {
                val getCredentialsMethod = assumedRoleProvider.getClass.getMethod("getCredentials")
                val credentials = getCredentialsMethod.invoke(assumedRoleProvider)

                // Extract username, password, and token using reflection
                val getUserNameMethod = credentials.getClass.getMethod("getUserName")
                val getPasswordMethod = credentials.getClass.getMethod("getPassword")
                val getTokenMethod = credentials.getClass.getMethod("getToken")

                val username = getUserNameMethod.invoke(credentials).toString
                val password = getPasswordMethod.invoke(credentials).toString
                val token = getTokenMethod.invoke(credentials)

                if (token != null) {
                  new BasicSessionCredentials(username, password, token.toString)
                } else {
                  new BasicAWSCredentials(username, password)
                }
              } catch {
                case e: Exception =>
                  logger.error("Failed to adapt AssumedRoleCredentialProvider", e)
                  throw e
              }
            }

            override def refresh(): Unit = {
              // Try to refresh the credentials if possible
              try {
                val refreshMethod = assumedRoleProvider.getClass.getMethod("refresh")
                refreshMethod.invoke(assumedRoleProvider)
              } catch {
                case _: Exception => // Ignore refresh failures
              }
            }
          }
        case other =>
          // For any other type, log a warning and try to adapt as best we can
          logger.warn(s"Unknown credential provider type: ${other.getClass.getName}")
          throw new IllegalArgumentException(
            s"Unsupported credential provider type: ${other.getClass.getName}"
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
          // Use the adapter method to handle potential AssumedRoleCredentialProvider
          try {
            builderWithEndpoint.withCredentials(adaptAssumedRoleCredentialProvider(cp))
          } catch {
            case e: Exception =>
              logger.warn(
                s"Failed to adapt credential provider, falling back to original: ${e.getMessage}"
              )
              builderWithEndpoint.withCredentials(cp)
          }
        case None => builderWithEndpoint
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
