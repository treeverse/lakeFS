package io.treeverse.clients

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.model.{HeadBucketRequest, Region}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.{AmazonServiceException, ClientConfiguration}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

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
    val StorageAccountKeyPropertyPattern =
      "fs.azure.account.key.<storageAccountName>.dfs.core.windows.net"
    val StorageAccNamePlaceHolder = "<storageAccountName>"
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
  }

  object S3 {
    val S3MaxBulkSize = 1000
    val S3NumRetries = 1000
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

      var client =
        initializeS3Client(configuration, credentialsProvider, awsS3ClientBuilder, endpoint, region)

      if (!validateClientAndBucketRegionsMatch(client, bucket)) {
        val bucketRegion = getAWSS3Region(client, bucket)
        logger.info(
          s"""Bucket "$bucket" is not in region "$region", discovered it in region "$bucketRegion"""
        )
        client = initializeS3Client(configuration,
                                    credentialsProvider,
                                    AmazonS3ClientBuilder.standard(),
                                    endpoint,
                                    bucketRegion
                                   )
      }
      client
    }

    private def initializeS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        endpoint: String,
        region: String
    ): AmazonS3 = {
      val builder = awsS3ClientBuilder
        .withClientConfiguration(configuration)

      val builderWithEndpoint =
        if (endpoint != null)
          builder.withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(endpoint, region)
          )
        else
          builder.withRegion(region)

      val builderWithCredentials = credentialsProvider match {
        case Some(cp) => builderWithEndpoint.withCredentials(cp)
        case None     => builderWithEndpoint
      }
      builderWithCredentials.build
    }

    private def validateClientAndBucketRegionsMatch(client: AmazonS3, bucket: String): Boolean = {
      try {
        client.headBucket(new HeadBucketRequest(bucket))
        true
      } catch {
        case e: AmazonServiceException =>
          logger.info("Bucket \"{}\" isn't reachable with error: {}",
                      bucket: Any,
                      e.getMessage: Any
                     )
          false
      }
    }

    private def getAWSS3Region(client: AmazonS3, bucket: String): String = {
      val bucketRegion = client.getBucketLocation(bucket)
      val region = Region.fromValue(bucketRegion)
      // The comparison `region.equals(Region.US_Standard))` is required due to AWS's backward compatibility:
      // https://github.com/aws/aws-sdk-java/issues/1470.
      // "us-east-1" was previously called "US Standard". This resulted in a return value of "US" when
      // calling `client.getBucketLocation(bucket)`.
      if (region.equals(Region.US_Standard)) "us-east-1"
      else region.toString
    }
  }
}
