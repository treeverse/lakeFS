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
  private val logger: Logger = LoggerFactory.getLogger(getClass)

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
      logger.info(s"Creating S3 client for bucket: $bucket, endpoint: $endpoint, region: $region")

      // DEBUG: Log all system properties related to AWS or S3 for debugging
      logger.info("S3-related System Properties:")
      System.getProperties
        .stringPropertyNames()
        .toArray
        .filter(_.toString.contains("s3") || _.toString.contains("aws"))
        .foreach(prop => {
          val key = prop.toString
          val value = if (key.toLowerCase.contains("secret") || key.toLowerCase.contains("key")) {
            "<CREDENTIAL REDACTED>"
          } else {
            System.getProperty(key)
          }
          logger.info(s"  $key=$value")
        })

      // Check for Hadoop's assumed role configuration
      val roleArn = System.getProperty("fs.s3a.assumed.role.arn")
      val sparkHadoopRoleArn = System.getProperty("spark.hadoop.fs.s3a.assumed.role.arn")
      val isAssumeRoleProvider = (roleArn != null && !roleArn.isEmpty) ||
        (sparkHadoopRoleArn != null && !sparkHadoopRoleArn.isEmpty)

      logger.info(s"Using role ARN? $isAssumeRoleProvider")
      if (isAssumeRoleProvider) {
        val actualRoleArn = if (roleArn != null && !roleArn.isEmpty) roleArn else sparkHadoopRoleArn
        logger.info(
          s"Using role ARN: $actualRoleArn, skipping bucket location check for EMR 7.0.0 compatibility"
        )
        val client =
          initializeS3Client(configuration, credentialsProvider, builder, endpoint, region)
        return client
      }

      // Standard flow for non-role based auth
      logger.info("Using standard credential flow")
      val client = initializeS3Client(configuration, credentialsProvider, builder, endpoint)

      var bucketRegion =
        try {
          logger.info("Attempting to get bucket location")
          val location = client.getBucketLocation(bucket)
          logger.info(
            s"Got bucket location: ${if (location == null || location.isEmpty) "null/empty"
            else location}"
          )
          if (location == null || location.isEmpty) null else location
        } catch {
          case e: Exception =>
            logger.info(f"Could not fetch region for bucket $bucket: ${e.getMessage}", e)
            ""
        }

      if (bucketRegion == "" && region == "") {
        logger.error(s"Could not determine region for bucket $bucket and no region provided")
        throw new IllegalArgumentException(
          s"""Could not fetch region for bucket "$bucket" and no region was provided"""
        )
      }

      if (bucketRegion == "") {
        logger.info(s"Using provided region: $region")
        bucketRegion = region
      } else {
        logger.info(s"Using bucket region: $bucketRegion")
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
      logger.info("Initializing S3 client:")
      logger.info(s"  Endpoint: $endpoint")
      logger.info(s"  Region: ${if (region == null) "null" else region}")
      logger.info(s"  Credentials provided: ${credentialsProvider.isDefined}")

      val configuredBuilder = builder.withClientConfiguration(configuration)

      if (endpoint != null && !endpoint.isEmpty) {
        logger.info(
          s"Setting endpoint configuration: $endpoint, region: ${if (region == null) "null"
          else region}"
        )
        configuredBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(endpoint, region)
        )
      } else if (region != null && !region.isEmpty) {
        logger.info(s"Setting region: $region")
        configuredBuilder.withRegion(region)
      }

      // Apply credentials if provided
      if (credentialsProvider.isDefined) {
        logger.info("Applying credentials provider to builder")
        configuredBuilder.withCredentials(credentialsProvider.get)
      } else {
        logger.info("No explicit credentials provided")
      }

      logger.info("Building S3 client")
      val client = configuredBuilder.build()
      logger.info("S3 client created successfully")
      client
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
