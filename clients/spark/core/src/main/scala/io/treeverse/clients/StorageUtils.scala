package io.treeverse.clients

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.model.{HeadBucketRequest, Region}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.{AmazonServiceException, ClientConfiguration}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.nio.charset.Charset

object StorageUtils {
  val StorageTypeS3 = "s3"
  val StorageTypeAzure = "azure"

  def concatKeysToStorageNamespace(
      keys: Seq[String],
      storageNamespace: String,
      storageType: String
  ): Seq[String] = {
    storageType match {
      case StorageTypeS3    => S3.concatKeysToStorageNamespace(keys, storageNamespace)
      case StorageTypeAzure => AzureBlob.concatKeysToStorageNamespace(keys, storageNamespace)
      case _ => throw new IllegalArgumentException("Unknown storage type " + storageType)
    }
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

    def concatKeysToStorageNamespace(keys: Seq[String], storageNamespace: String): Seq[String] = {
      val addSuffixSlash =
        if (storageNamespace.endsWith("/")) storageNamespace else storageNamespace.concat("/")

      if (keys.isEmpty) return Seq.empty
      keys
        .map(x => addSuffixSlash.concat(x))
        .map(x => x.getBytes(Charset.forName("UTF-8")))
        .map(x => new String(x))
    }
  }

  object S3 {
    val S3MaxBulkSize = 1000
    val S3NumRetries = 1000
    val logger: Logger = LoggerFactory.getLogger(getClass.toString)

    def concatKeysToStorageNamespace(keys: Seq[String], storageNamespace: String): Seq[String] = {
      val addSuffixSlash =
        if (storageNamespace.endsWith("/")) storageNamespace else storageNamespace.concat("/")
      val snPrefix =
        if (addSuffixSlash.startsWith("/")) addSuffixSlash.substring(1) else addSuffixSlash

      if (keys.isEmpty) return Seq.empty
      keys.map(x => snPrefix.concat(x))
    }

    def concatKeysToStorageNamespacePrefix(
        keys: Seq[String],
        storageNamespace: String
    ): Seq[String] = {
      val uri = new URI(storageNamespace)
      val key = uri.getPath
      val addSuffixSlash = if (key.endsWith("/")) key else key.concat("/")
      val snPrefix =
        if (addSuffixSlash.startsWith("/")) addSuffixSlash.substring(1) else addSuffixSlash

      if (keys.isEmpty) return Seq.empty
      keys.map(x => snPrefix.concat(x))
    }

    def createAndValidateS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        region: String,
        bucket: String
    ): AmazonS3 = {

      require(awsS3ClientBuilder != null)
      require(bucket.nonEmpty)

      var client =
        initializeS3Client(configuration, credentialsProvider, awsS3ClientBuilder, region)

      if (!validateClientAndBucketRegionsMatch(client, bucket)) {
        val bucketRegion = getAWSS3Region(client, bucket)
        logger.info(
          s"""Bucket "$bucket" is not in region "$region", discovered it in region "$bucketRegion"""
        )
        client = initializeS3Client(configuration,
                                    credentialsProvider,
                                    AmazonS3ClientBuilder.standard(),
                                    bucketRegion
                                   )
      }
      client
    }

    private def initializeS3Client(
        configuration: ClientConfiguration,
        credentialsProvider: Option[AWSCredentialsProvider],
        awsS3ClientBuilder: AmazonS3ClientBuilder,
        region: String
    ): AmazonS3 = {
      val builder = awsS3ClientBuilder
        .withClientConfiguration(configuration)
        .withRegion(region)
      val builderWithCredentials = credentialsProvider match {
        case Some(cp) => builder.withCredentials(cp)
        case None     => builder
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
