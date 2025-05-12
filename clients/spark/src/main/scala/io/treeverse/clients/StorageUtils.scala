package io.treeverse.clients

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.retry.PredefinedRetryPolicies.SDKDefaultRetryCondition
import com.amazonaws.retry.RetryUtils
import com.amazonaws.services.s3.model.{Region, GetBucketLocationRequest}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws._
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.lang.reflect.Method
import java.util.concurrent.TimeUnit
import scala.util.Try

object StorageUtils {
  val StorageTypeS3 = "s3"
  val StorageTypeAzure = "azure"

  // Initialize with version logging
  private val logger: Logger = LoggerFactory.getLogger(getClass.toString)
  logEnvironmentInfo()

  /** Log detailed information about the environment and class versions */
  private def logEnvironmentInfo(): Unit = {
    try {
      logger.info("=== Environment Information ===")

      // Log Java version
      val javaVersion = System.getProperty("java.version")
      val javaVendor = System.getProperty("java.vendor")
      logger.info(s"Java: $javaVersion ($javaVendor)")

      // Log AWS SDK version
      try {
        val awsVersion = com.amazonaws.util.VersionInfoUtils.getVersion()
        logger.info(s"AWS SDK: version=$awsVersion")
      } catch {
        case e: Throwable => logger.info(s"AWS SDK version: Unable to determine: ${e.getMessage}")
      }

      // Log class availability
      val classesToCheck = List(
        "com.amazonaws.auth.AWSCredentialsProvider",
        "com.amazonaws.services.s3.AmazonS3",
        "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
      )

      classesToCheck.foreach { className =>
        try {
          val clazz = Class.forName(className)
          val location = Option(clazz.getProtectionDomain.getCodeSource)
            .flatMap(cs => Option(cs.getLocation))
            .map(_.toString)
            .getOrElse("unknown")
          logger.info(s"Class: $className, location=$location")
        } catch {
          case _: ClassNotFoundException =>
            logger.info(s"Class: $className is not available")
          case e: Throwable =>
            logger.info(s"Class $className: Error getting info: ${e.getMessage}")
        }
      }

      logger.info("=== End Environment Information ===")
    } catch {
      case e: Throwable =>
        logger.warn(s"Failed to log environment information: ${e.getMessage}", e)
    }
  }

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
        credentialsProvider: Option[Any], // Changed to Any to accept any credential type
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String,
        bucket: String
    ): AmazonS3 = {
      require(awsS3ClientBuilder != null)
      require(bucket.nonEmpty)

      // Log credential provider details
      if (credentialsProvider.isDefined) {
        val provider = credentialsProvider.get
        logger.info(s"Credential provider: ${provider.getClass.getName}")
      }

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

      logger.info(s"Using region $bucketRegion for bucket $bucket")
      initializeS3Client(configuration,
                         credentialsProvider,
                         awsS3ClientBuilder,
                         endpoint,
                         bucketRegion
                        )
    }

    private def initializeS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[Any], // Changed to Any
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

      // Apply credentials with reflection for compatibility with EMR 7.0.0
      val builderWithCredentials = credentialsProvider match {
        case Some(provider) =>
          applyCredentials(builderWithEndpoint, provider)
          builderWithEndpoint
        case None =>
          builderWithEndpoint
      }

      builderWithCredentials.build
    }

    private def getAWSS3Region(client: AmazonS3, bucket: String): String = {
      var request = new GetBucketLocationRequest(bucket)
      request = request.withSdkClientExecutionTimeout(TimeUnit.SECONDS.toMillis(1).intValue())
      val bucketRegion = client.getBucketLocation(request)
      Try(Region.fromValue(bucketRegion).toAWSRegion().getName()).getOrElse("")
    }

    // Helper method to safely apply credentials to the builder
    private def applyCredentials(builder: AmazonS3ClientBuilder, provider: Any): Unit = {
      provider match {
        // If it's already the right type, use it directly
        case awsProvider: AWSCredentialsProvider =>
          logger.info(
            s"Using AWS SDK v1 credentials provider directly: ${awsProvider.getClass.getName}"
          )
          builder.withCredentials(awsProvider)

        // If it's a Hadoop's AssumedRoleCredentialProvider, extract AWS credentials via reflection
        case _
            if provider.getClass.getName == "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider" =>
          logger.info("Extracting credentials from AssumedRoleCredentialProvider via reflection")
          try {
            // Use reflection to get credentials from the provider
            val getCredentialsMethod = provider.getClass.getMethod("getCredentials")
            val credentials = getCredentialsMethod.invoke(provider)

            // Extract access key and secret key using reflection
            val accessKeyMethod = credentials.getClass.getMethod("getAWSAccessKeyId")
            val secretKeyMethod = credentials.getClass.getMethod("getAWSSecretKey")

            val accessKey = accessKeyMethod.invoke(credentials).toString
            val secretKey = secretKeyMethod.invoke(credentials).toString

            // Create a basic credentials provider with the keys
            val basicCreds = new BasicAWSCredentials(accessKey, secretKey)
            builder.withCredentials(new AWSCredentialsProvider {
              override def getCredentials: AWSCredentials = basicCreds
              override def refresh(): Unit = {}
            })

            logger.info("Successfully adapted Hadoop S3A credentials to AWS SDK credentials")
          } catch {
            case e: Exception =>
              logger.warn(
                s"Failed to adapt credentials from ${provider.getClass.getName}: ${e.getMessage}",
                e
              )
          }

        // For other types, try to extract credentials using common methods
        case _ =>
          logger.info(
            s"Attempting to extract credentials from unknown provider: ${provider.getClass.getName}"
          )
          try {
            // Try common credential getter methods
            val methods = provider.getClass.getMethods
            val getCredentialsMethod = methods.find(_.getName == "getCredentials")

            if (getCredentialsMethod.isDefined) {
              val credentials = getCredentialsMethod.get.invoke(provider)

              // Try to get access key and secret key
              val credClass = credentials.getClass
              val accessKeyMethod =
                findMethodByNames(credClass, "getAWSAccessKeyId", "getAccessKeyId")
              val secretKeyMethod = findMethodByNames(credClass, "getAWSSecretKey", "getSecretKey")

              if (accessKeyMethod.isDefined && secretKeyMethod.isDefined) {
                val accessKey = accessKeyMethod.get.invoke(credentials).toString
                val secretKey = secretKeyMethod.get.invoke(credentials).toString

                val basicCreds = new BasicAWSCredentials(accessKey, secretKey)
                builder.withCredentials(new AWSCredentialsProvider {
                  override def getCredentials: AWSCredentials = basicCreds
                  override def refresh(): Unit = {}
                })

                logger.info(
                  s"Successfully adapted ${provider.getClass.getName} to AWS SDK credentials"
                )
              }
            }
          } catch {
            case e: Exception =>
              logger.warn(
                s"Failed to extract credentials from ${provider.getClass.getName}: ${e.getMessage}",
                e
              )
          }
      }
    }

    // Helper method to find a method by multiple possible names
    private def findMethodByNames(clazz: Class[_], names: String*): Option[Method] = {
      names
        .flatMap(name =>
          try {
            Some(clazz.getMethod(name))
          } catch {
            case _: NoSuchMethodException => None
          }
        )
        .headOption
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
