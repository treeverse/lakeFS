package io.treeverse.clients

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{HeadBucketRequest, AmazonS3Exception}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.AmazonClientException
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
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
        clientConfig: ClientConfiguration,
        credentialsProvider: Option[Any], // Changed to Any to accept any type
        builder: AmazonS3ClientBuilder,
        endpoint: String,
        regionName: String,
        bucket: String
    ): AmazonS3 = {
      require(bucket.nonEmpty)

      // First create a temp client to check bucket location
      val tempBuilder = AmazonS3ClientBuilder
        .standard()
        .withClientConfiguration(clientConfig)
        .withPathStyleAccessEnabled(true)

      // Apply credentials if provided, handling different types
      applyCredentials(tempBuilder, credentialsProvider)

      // Configure endpoint or region
      val normalizedRegion = normalizeRegionName(regionName)
      if (endpoint != null && !endpoint.isEmpty) {
        tempBuilder.withEndpointConfiguration(
          new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(endpoint,
                                                                                  normalizedRegion
                                                                                 )
        )
      } else if (normalizedRegion != null && !normalizedRegion.isEmpty) {
        tempBuilder.withRegion(normalizedRegion)
      }

      // Get bucket's actual region
      var bucketRegion = regionName
      var bucketExists = false

      try {
        val tempClient = tempBuilder.build()
        val location = tempClient.getBucketLocation(bucket)
        bucketRegion = if (location == null || location.isEmpty) null else location
        bucketExists = true
      } catch {
        case e: Exception =>
          logger.info(f"Could not fetch info for bucket $bucket", e)
      }

      // If we can't determine bucket region and no region was provided, fail
      if (!bucketExists && (regionName == null || regionName.isEmpty)) {
        throw new IllegalArgumentException(
          s"""Could not fetch region for bucket "$bucket" and no region was provided"""
        )
      }

      // Now create the final client with the bucket's region
      builder.withClientConfiguration(clientConfig)
      applyCredentials(builder, credentialsProvider)

      if (endpoint != null && !endpoint.isEmpty) {
        builder.withEndpointConfiguration(
          new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(endpoint,
                                                                                  bucketRegion
                                                                                 )
        )
      } else if (bucketRegion != null && !bucketRegion.isEmpty) {
        builder.withRegion(bucketRegion)
      }

      builder.build()
    }

    // Helper method to safely apply credentials to the builder
    private def applyCredentials(
        builder: AmazonS3ClientBuilder,
        credentialsProvider: Option[Any]
    ): Unit = {
      if (credentialsProvider.isEmpty) {
        return
      }

      val provider = credentialsProvider.get

      provider match {
        // If it's already the right type, use it directly
        case awsProvider: AWSCredentialsProvider =>
          builder.withCredentials(awsProvider)

        // If it's a Hadoop's AssumedRoleCredentialProvider, extract AWS credentials via reflection
        case _
            if provider.getClass.getName == "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider" =>
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
              logger.warn(s"Failed to adapt credentials from ${provider.getClass.getName}", e)
          }

        // For other types, try to extract credentials using common methods
        case _ =>
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
              logger.warn(s"Failed to extract credentials from ${provider.getClass.getName}", e)
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

    // Helper method to normalize region names between SDK v1 and v2
    private def normalizeRegionName(regionName: String): String = {
      if (regionName == null || regionName.isEmpty) {
        return null
      }

      // Special case: US_STANDARD is a legacy alias for US_EAST_1
      if (regionName.equalsIgnoreCase("US") || regionName.equalsIgnoreCase("US_STANDARD")) {
        return "us-east-1"
      }

      // Convert SDK v2 uppercase with underscores to SDK v1 lowercase with hyphens
      regionName.toLowerCase.replace("_", "-")
    }
  }
}

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
