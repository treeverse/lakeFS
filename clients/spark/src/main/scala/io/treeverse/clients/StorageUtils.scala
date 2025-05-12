package io.treeverse.clients

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{HeadBucketRequest, AmazonS3Exception}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.AmazonClientException
import com.amazonaws.{SDKGlobalConfiguration, VersionInfoUtils}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.lang.reflect.Method
import java.util.{Properties, Enumeration}
import scala.collection.JavaConverters._

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
        val awsVersion = VersionInfoUtils.getVersion()
        val userAgent = VersionInfoUtils.getUserAgent()
        logger.info(s"AWS SDK: version=$awsVersion, userAgent=$userAgent")
      } catch {
        case e: Throwable => logger.info(s"AWS SDK version: Unable to determine: ${e.getMessage}")
      }

      // Log AWS SDK Configuration
      try {
        val signerOverrideSystem = System.getProperty(SDKGlobalConfiguration.SIGNER_OVERRIDE_SYSTEM_PROPERTY)
        logger.info(s"AWS SDK Signer Override: $signerOverrideSystem")

        val regionOverride = System.getProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY)
        logger.info(s"AWS Region Override: $regionOverride")
      } catch {
        case e: Throwable => logger.info(s"AWS SDK Config: Unable to determine: ${e.getMessage}")
      }

      // Log key package versions
      val packagesToCheck = List(
        "com.amazonaws",
        "software.amazon.awssdk",
        "org.apache.hadoop",
        "org.apache.hadoop.fs.s3a",
        "io.treeverse.clients"
      )

      packagesToCheck.foreach { pkgName =>
        try {
          val pkg = Package.getPackage(pkgName)
          if (pkg != null) {
            val version = Option(pkg.getImplementationVersion).getOrElse("unknown")
            val vendor = Option(pkg.getImplementationVendor).getOrElse("unknown")
            logger.info(s"Package: $pkgName, version=$version, vendor=$vendor")
          } else {
            logger.info(s"Package: $pkgName is not loaded")
          }
        } catch {
          case e: Throwable => logger.info(s"Package $pkgName: Error getting info: ${e.getMessage}")
        }
      }

      // Log class availability and locations
      val classesToCheck = List(
        "com.amazonaws.auth.AWSCredentialsProvider",
        "com.amazonaws.services.s3.AmazonS3",
        "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider",
        "software.amazon.awssdk.services.s3.S3Client",
        "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
        "io.treeverse.clients.StorageUtils"
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

      // Log system properties
      logger.info("=== System Properties ===")
      val props = System.getProperties.asScala.toList.sortBy(_._1)
      props.foreach { case (key, value) =>
        if (key.contains("aws") || key.contains("hadoop") || key.contains("s3") ||
          key.contains("spark") || key.contains("emr") || key.contains("java")) {
          logger.info(s"System Property: $key = $value")
        }
      }

      // Log class loaders
      logger.info("=== ClassLoader Hierarchy ===")
      var classLoader = getClass.getClassLoader
      var level = 0
      while (classLoader != null) {
        logger.info(s"ClassLoader L$level: ${classLoader.getClass.getName}")
        classLoader = classLoader.getParent
        level += 1
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

      // Log credential provider details
      if (credentialsProvider.isDefined) {
        val provider = credentialsProvider.get
        logger.info(s"Credential provider: ${provider.getClass.getName}")

        // Log detailed info about the provider
        try {
          val methods = provider.getClass.getMethods
            .filter(m => m.getParameterCount == 0 && !m.getName.equals("toString"))
            .map(_.getName)
            .sorted
          logger.info(s"Credential provider available methods: ${methods.mkString(", ")}")
        } catch {
          case e: Exception =>
            logger.info(s"Error inspecting credential provider: ${e.getMessage}")
        }
      } else {
        logger.info("No credential provider specified")
      }

      // First create a temp client to check bucket location
      val tempBuilder = AmazonS3ClientBuilder.standard()
        .withClientConfiguration(clientConfig)
        .withPathStyleAccessEnabled(true)

      // Apply credentials if provided, handling different types
      applyCredentials(tempBuilder, credentialsProvider)

      // Configure endpoint or region
      val normalizedRegion = normalizeRegionName(regionName)
      if (endpoint != null && !endpoint.isEmpty) {
        logger.info(s"Using endpoint: $endpoint with region: $normalizedRegion")
        tempBuilder.withEndpointConfiguration(
          new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(endpoint, normalizedRegion)
        )
      } else if (normalizedRegion != null && !normalizedRegion.isEmpty) {
        logger.info(s"Using region: $normalizedRegion")
        tempBuilder.withRegion(normalizedRegion)
      }

      // Get bucket's actual region
      var bucketRegion = regionName
      var bucketExists = false

      try {
        val tempClient = tempBuilder.build()
        logger.info(s"Checking location for bucket: $bucket")
        val location = tempClient.getBucketLocation(bucket)
        logger.info(s"Bucket $bucket location: $location")
        bucketRegion = if (location == null || location.isEmpty) null else location
        bucketExists = true
      } catch {
        case e: Exception =>
          logger.info(f"Could not fetch info for bucket $bucket: ${e.getMessage}", e)
      }

      // If we can't determine bucket region and no region was provided, fail
      if (!bucketExists && (regionName == null || regionName.isEmpty)) {
        throw new IllegalArgumentException(
          s"""Could not fetch region for bucket "$bucket" and no region was provided"""
        )
      }

      // Now create the final client with the bucket's region
      logger.info(s"Creating final S3 client with region: $bucketRegion")
      builder.withClientConfiguration(clientConfig)
      applyCredentials(builder, credentialsProvider)

      if (endpoint != null && !endpoint.isEmpty) {
        builder.withEndpointConfiguration(
          new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(endpoint, bucketRegion)
        )
      } else if (bucketRegion != null && !bucketRegion.isEmpty) {
        builder.withRegion(bucketRegion)
      }

      val client = builder.build()
      logger.info(s"S3 client created successfully: ${client.getClass.getName}")
      client
    }

    // Helper method to safely apply credentials to the builder
    private def applyCredentials(builder: AmazonS3ClientBuilder, credentialsProvider: Option[Any]): Unit = {
      if (credentialsProvider.isEmpty) {
        logger.info("No credentials to apply")
        return
      }

      val provider = credentialsProvider.get

      provider match {
        // If it's already the right type, use it directly
        case awsProvider: AWSCredentialsProvider =>
          logger.info(s"Using AWS SDK v1 credentials provider directly: ${awsProvider.getClass.getName}")
          builder.withCredentials(awsProvider)

        // If it's a Hadoop's AssumedRoleCredentialProvider, extract AWS credentials via reflection
        case _ if provider.getClass.getName == "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider" =>
          logger.info("Extracting credentials from AssumedRoleCredentialProvider via reflection")
          try {
            // Use reflection to get credentials from the provider
            val getCredentialsMethod = provider.getClass.getMethod("getCredentials")
            logger.info(s"Found getCredentials method: ${getCredentialsMethod}")
            val credentials = getCredentialsMethod.invoke(provider)
            logger.info(s"Credentials object type: ${credentials.getClass.getName}")

            // Extract access key and secret key using reflection
            val accessKeyMethod = credentials.getClass.getMethod("getAWSAccessKeyId")
            val secretKeyMethod = credentials.getClass.getMethod("getAWSSecretKey")
            logger.info(s"Found credential methods: ${accessKeyMethod.getName}, ${secretKeyMethod.getName}")

            val accessKey = accessKeyMethod.invoke(credentials).toString
            val secretKey = secretKeyMethod.invoke(credentials).toString
            logger.info("Successfully extracted access key and secret key")

            // Create a basic credentials provider with the keys
            val basicCreds = new BasicAWSCredentials(accessKey, secretKey)
            builder.withCredentials(new AWSCredentialsProvider {
              override def getCredentials: AWSCredentials = basicCreds
              override def refresh(): Unit = {}
            })

            logger.info("Successfully adapted Hadoop S3A credentials to AWS SDK credentials")
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to adapt credentials from ${provider.getClass.getName}: ${e.getMessage}", e)
              logger.warn("Will continue without explicit credentials")
          }

        // For other types, try to extract credentials using common methods
        case _ =>
          logger.info(s"Attempting to extract credentials from unknown provider: ${provider.getClass.getName}")
          try {
            // Try common credential getter methods
            val methods = provider.getClass.getMethods
            val getCredentialsMethod = methods.find(_.getName == "getCredentials")

            if (getCredentialsMethod.isDefined) {
              logger.info(s"Found getCredentials method: ${getCredentialsMethod.get}")
              val credentials = getCredentialsMethod.get.invoke(provider)
              logger.info(s"Credentials object type: ${credentials.getClass.getName}")

              // Try to get access key and secret key
              val credClass = credentials.getClass
              val accessKeyMethod = findMethodByNames(credClass, "getAWSAccessKeyId", "getAccessKeyId")
              val secretKeyMethod = findMethodByNames(credClass, "getAWSSecretKey", "getSecretKey")

              if (accessKeyMethod.isDefined && secretKeyMethod.isDefined) {
                logger.info(s"Found credential methods: ${accessKeyMethod.get.getName}, ${secretKeyMethod.get.getName}")
                val accessKey = accessKeyMethod.get.invoke(credentials).toString
                val secretKey = secretKeyMethod.get.invoke(credentials).toString
                logger.info("Successfully extracted access key and secret key")

                val basicCreds = new BasicAWSCredentials(accessKey, secretKey)
                builder.withCredentials(new AWSCredentialsProvider {
                  override def getCredentials: AWSCredentials = basicCreds
                  override def refresh(): Unit = {}
                })

                logger.info(s"Successfully adapted ${provider.getClass.getName} to AWS SDK credentials")
              } else {
                logger.warn(s"Could not find access/secret key methods on credentials object")
              }
            } else {
              logger.warn(s"Could not find getCredentials method on provider")
            }
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to extract credentials from ${provider.getClass.getName}: ${e.getMessage}", e)
              logger.warn("Will continue without explicit credentials")
          }
      }
    }

    // Helper method to find a method by multiple possible names
    private def findMethodByNames(clazz: Class[_], names: String*): Option[Method] = {
      names.flatMap(name =>
        try {
          Some(clazz.getMethod(name))
        } catch {
          case _: NoSuchMethodException => None
        }
      ).headOption
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
        } else if (s3e.getStatusCode == 429 ||
          (s3e.getStatusCode >= 500 && s3e.getStatusCode < 600)) {
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
}package io.treeverse.clients

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{HeadBucketRequest, AmazonS3Exception}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.AmazonClientException
import com.amazonaws.{SDKGlobalConfiguration, VersionInfoUtils}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.lang.reflect.Method
import java.util.{Properties, Enumeration}
import scala.collection.JavaConverters._

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
        val awsVersion = VersionInfoUtils.getVersion()
        val userAgent = VersionInfoUtils.getUserAgent()
        logger.info(s"AWS SDK: version=$awsVersion, userAgent=$userAgent")
      } catch {
        case e: Throwable => logger.info(s"AWS SDK version: Unable to determine: ${e.getMessage}")
      }

      // Log AWS SDK Configuration
      try {
        val signerOverrideSystem = System.getProperty(SDKGlobalConfiguration.SIGNER_OVERRIDE_SYSTEM_PROPERTY)
        logger.info(s"AWS SDK Signer Override: $signerOverrideSystem")

        val regionOverride = System.getProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY)
        logger.info(s"AWS Region Override: $regionOverride")
      } catch {
        case e: Throwable => logger.info(s"AWS SDK Config: Unable to determine: ${e.getMessage}")
      }

      // Log key package versions
      val packagesToCheck = List(
        "com.amazonaws",
        "software.amazon.awssdk",
        "org.apache.hadoop",
        "org.apache.hadoop.fs.s3a",
        "io.treeverse.clients"
      )

      packagesToCheck.foreach { pkgName =>
        try {
          val pkg = Package.getPackage(pkgName)
          if (pkg != null) {
            val version = Option(pkg.getImplementationVersion).getOrElse("unknown")
            val vendor = Option(pkg.getImplementationVendor).getOrElse("unknown")
            logger.info(s"Package: $pkgName, version=$version, vendor=$vendor")
          } else {
            logger.info(s"Package: $pkgName is not loaded")
          }
        } catch {
          case e: Throwable => logger.info(s"Package $pkgName: Error getting info: ${e.getMessage}")
        }
      }

      // Log class availability and locations
      val classesToCheck = List(
        "com.amazonaws.auth.AWSCredentialsProvider",
        "com.amazonaws.services.s3.AmazonS3",
        "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider",
        "software.amazon.awssdk.services.s3.S3Client",
        "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
        "io.treeverse.clients.StorageUtils"
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

      // Log system properties
      logger.info("=== System Properties ===")
      val props = System.getProperties.asScala.toList.sortBy(_._1)
      props.foreach { case (key, value) =>
        if (key.contains("aws") || key.contains("hadoop") || key.contains("s3") ||
          key.contains("spark") || key.contains("emr") || key.contains("java")) {
          logger.info(s"System Property: $key = $value")
        }
      }

      // Log class loaders
      logger.info("=== ClassLoader Hierarchy ===")
      var classLoader = getClass.getClassLoader
      var level = 0
      while (classLoader != null) {
        logger.info(s"ClassLoader L$level: ${classLoader.getClass.getName}")
        classLoader = classLoader.getParent
        level += 1
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

      // Log credential provider details
      if (credentialsProvider.isDefined) {
        val provider = credentialsProvider.get
        logger.info(s"Credential provider: ${provider.getClass.getName}")

        // Log detailed info about the provider
        try {
          val methods = provider.getClass.getMethods
            .filter(m => m.getParameterCount == 0 && !m.getName.equals("toString"))
            .map(_.getName)
            .sorted
          logger.info(s"Credential provider available methods: ${methods.mkString(", ")}")
        } catch {
          case e: Exception =>
            logger.info(s"Error inspecting credential provider: ${e.getMessage}")
        }
      } else {
        logger.info("No credential provider specified")
      }

      // First create a temp client to check bucket location
      val tempBuilder = AmazonS3ClientBuilder.standard()
        .withClientConfiguration(clientConfig)
        .withPathStyleAccessEnabled(true)

      // Apply credentials if provided, handling different types
      applyCredentials(tempBuilder, credentialsProvider)

      // Configure endpoint or region
      val normalizedRegion = normalizeRegionName(regionName)
      if (endpoint != null && !endpoint.isEmpty) {
        logger.info(s"Using endpoint: $endpoint with region: $normalizedRegion")
        tempBuilder.withEndpointConfiguration(
          new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(endpoint, normalizedRegion)
        )
      } else if (normalizedRegion != null && !normalizedRegion.isEmpty) {
        logger.info(s"Using region: $normalizedRegion")
        tempBuilder.withRegion(normalizedRegion)
      }

      // Get bucket's actual region
      var bucketRegion = regionName
      var bucketExists = false

      try {
        val tempClient = tempBuilder.build()
        logger.info(s"Checking location for bucket: $bucket")
        val location = tempClient.getBucketLocation(bucket)
        logger.info(s"Bucket $bucket location: $location")
        bucketRegion = if (location == null || location.isEmpty) null else location
        bucketExists = true
      } catch {
        case e: Exception =>
          logger.info(f"Could not fetch info for bucket $bucket: ${e.getMessage}", e)
      }

      // If we can't determine bucket region and no region was provided, fail
      if (!bucketExists && (regionName == null || regionName.isEmpty)) {
        throw new IllegalArgumentException(
          s"""Could not fetch region for bucket "$bucket" and no region was provided"""
        )
      }

      // Now create the final client with the bucket's region
      logger.info(s"Creating final S3 client with region: $bucketRegion")
      builder.withClientConfiguration(clientConfig)
      applyCredentials(builder, credentialsProvider)

      if (endpoint != null && !endpoint.isEmpty) {
        builder.withEndpointConfiguration(
          new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(endpoint, bucketRegion)
        )
      } else if (bucketRegion != null && !bucketRegion.isEmpty) {
        builder.withRegion(bucketRegion)
      }

      val client = builder.build()
      logger.info(s"S3 client created successfully: ${client.getClass.getName}")
      client
    }

    // Helper method to safely apply credentials to the builder
    private def applyCredentials(builder: AmazonS3ClientBuilder, credentialsProvider: Option[Any]): Unit = {
      if (credentialsProvider.isEmpty) {
        logger.info("No credentials to apply")
        return
      }

      val provider = credentialsProvider.get

      provider match {
        // If it's already the right type, use it directly
        case awsProvider: AWSCredentialsProvider =>
          logger.info(s"Using AWS SDK v1 credentials provider directly: ${awsProvider.getClass.getName}")
          builder.withCredentials(awsProvider)

        // If it's a Hadoop's AssumedRoleCredentialProvider, extract AWS credentials via reflection
        case _ if provider.getClass.getName == "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider" =>
          logger.info("Extracting credentials from AssumedRoleCredentialProvider via reflection")
          try {
            // Use reflection to get credentials from the provider
            val getCredentialsMethod = provider.getClass.getMethod("getCredentials")
            logger.info(s"Found getCredentials method: ${getCredentialsMethod}")
            val credentials = getCredentialsMethod.invoke(provider)
            logger.info(s"Credentials object type: ${credentials.getClass.getName}")

            // Extract access key and secret key using reflection
            val accessKeyMethod = credentials.getClass.getMethod("getAWSAccessKeyId")
            val secretKeyMethod = credentials.getClass.getMethod("getAWSSecretKey")
            logger.info(s"Found credential methods: ${accessKeyMethod.getName}, ${secretKeyMethod.getName}")

            val accessKey = accessKeyMethod.invoke(credentials).toString
            val secretKey = secretKeyMethod.invoke(credentials).toString
            logger.info("Successfully extracted access key and secret key")

            // Create a basic credentials provider with the keys
            val basicCreds = new BasicAWSCredentials(accessKey, secretKey)
            builder.withCredentials(new AWSCredentialsProvider {
              override def getCredentials: AWSCredentials = basicCreds
              override def refresh(): Unit = {}
            })

            logger.info("Successfully adapted Hadoop S3A credentials to AWS SDK credentials")
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to adapt credentials from ${provider.getClass.getName}: ${e.getMessage}", e)
              logger.warn("Will continue without explicit credentials")
          }

        // For other types, try to extract credentials using common methods
        case _ =>
          logger.info(s"Attempting to extract credentials from unknown provider: ${provider.getClass.getName}")
          try {
            // Try common credential getter methods
            val methods = provider.getClass.getMethods
            val getCredentialsMethod = methods.find(_.getName == "getCredentials")

            if (getCredentialsMethod.isDefined) {
              logger.info(s"Found getCredentials method: ${getCredentialsMethod.get}")
              val credentials = getCredentialsMethod.get.invoke(provider)
              logger.info(s"Credentials object type: ${credentials.getClass.getName}")

              // Try to get access key and secret key
              val credClass = credentials.getClass
              val accessKeyMethod = findMethodByNames(credClass, "getAWSAccessKeyId", "getAccessKeyId")
              val secretKeyMethod = findMethodByNames(credClass, "getAWSSecretKey", "getSecretKey")

              if (accessKeyMethod.isDefined && secretKeyMethod.isDefined) {
                logger.info(s"Found credential methods: ${accessKeyMethod.get.getName}, ${secretKeyMethod.get.getName}")
                val accessKey = accessKeyMethod.get.invoke(credentials).toString
                val secretKey = secretKeyMethod.get.invoke(credentials).toString
                logger.info("Successfully extracted access key and secret key")

                val basicCreds = new BasicAWSCredentials(accessKey, secretKey)
                builder.withCredentials(new AWSCredentialsProvider {
                  override def getCredentials: AWSCredentials = basicCreds
                  override def refresh(): Unit = {}
                })

                logger.info(s"Successfully adapted ${provider.getClass.getName} to AWS SDK credentials")
              } else {
                logger.warn(s"Could not find access/secret key methods on credentials object")
              }
            } else {
              logger.warn(s"Could not find getCredentials method on provider")
            }
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to extract credentials from ${provider.getClass.getName}: ${e.getMessage}", e)
              logger.warn("Will continue without explicit credentials")
          }
      }
    }

    // Helper method to find a method by multiple possible names
    private def findMethodByNames(clazz: Class[_], names: String*): Option[Method] = {
      names.flatMap(name =>
        try {
          Some(clazz.getMethod(name))
        } catch {
          case _: NoSuchMethodException => None
        }
      ).headOption
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
        } else if (s3e.getStatusCode == 429 ||
          (s3e.getStatusCode >= 500 && s3e.getStatusCode < 600)) {
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