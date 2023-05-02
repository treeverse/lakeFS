package io.treeverse.clients

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.azure.core.http.HttpClient
import com.azure.identity.{ClientSecretCredential, ClientSecretCredentialBuilder}
import com.azure.storage.blob.batch.{BlobBatchClient, BlobBatchClientBuilder}
import com.azure.storage.blob.{BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.RequestRetryOptions
import io.treeverse.clients.StorageUtils.AzureBlob._
import io.treeverse.clients.StorageUtils.{StorageTypeAzure, StorageTypeS3}
import org.apache.hadoop.conf.Configuration

import java.io.ByteArrayInputStream
import java.net.URI

trait StorageClient {
  def logRunID(runID: String, storageNamespace: String, region: String): Unit
}

object StorageClients {
  private def getRunIDMarkerLocation(runID: String, storagePrefix: String): String = {
    var prefix: String = storagePrefix.stripPrefix("/").stripSuffix("/")
    prefix = if (prefix.nonEmpty) prefix.concat("/_lakefs") else "_lakefs"
    s"$prefix/retention/gc/run_ids/$runID"
  }

  class S3(hc: Configuration) extends StorageClient {
    def getS3Client(
        bucket: String,
        region: String,
        numRetries: Int
    ): AmazonS3 =
      io.treeverse.clients.conditional.S3ClientBuilder.build(hc, bucket, region, numRetries)

    override def logRunID(runID: String, storageNamespace: String, region: String): Unit = {
      val uri = new URI(storageNamespace)
      val runIDMarkerInputStream = new ByteArrayInputStream(new Array[Byte](0))
      val bucket = uri.getHost
      val s3Client = getS3Client(bucket, region, StorageUtils.S3.S3NumRetries)
      val meta = new ObjectMetadata()
      meta.setContentLength(0)
      s3Client.putObject(bucket,
                         getRunIDMarkerLocation(runID, uri.getPath),
                         runIDMarkerInputStream,
                         meta
                        )
    }
  }

  class Azure(hc: Configuration) extends StorageClient {
    def getBlobContainerClient(
        storageAccountUrl: String,
        storageAccountName: String,
        containerName: String
    ): BlobContainerClient = {
      val blobServiceClient: BlobServiceClient =
        getBlobServiceClient(storageAccountUrl, storageAccountName)
      blobServiceClient.getBlobContainerClient(containerName)
    }
    def getBlobBatchClient(
        storageAccountUrl: String,
        storageAccountName: String
    ): BlobBatchClient = {
      val blobServiceClient: BlobServiceClient =
        getBlobServiceClient(storageAccountUrl, storageAccountName)
      new BlobBatchClientBuilder(blobServiceClient).buildClient
    }

    override def logRunID(runID: String, storageNamespace: String, region: String): Unit = {
      val uri = new URI(storageNamespace)
      val runIDMarkerInputStream = new ByteArrayInputStream(new Array[Byte](0))
      val storageAccountUrl = StorageUtils.AzureBlob.uriToStorageAccountUrl(uri)
      val storageAccountName = StorageUtils.AzureBlob.uriToStorageAccountName(uri)
      val containerName = StorageUtils.AzureBlob.uriToContainerName(uri)
      val blobClient =
        getBlobContainerClient(storageAccountUrl, storageAccountName, containerName)
      val pathArray = uri.getPath.split("/")
      val key = pathArray.slice(2, pathArray.length).reduce((a1, a2) => a1 + "/" + a2)
      blobClient
        .getBlobClient(getRunIDMarkerLocation(runID, key))
        .upload(runIDMarkerInputStream, 0)
    }

    private def getBlobServiceClient(
        storageAccountUrl: String,
        storageAccountName: String
    ): BlobServiceClient = {
      val storageAccountKey = hc.get(String.format(StorageAccountKeyProperty, storageAccountName))
      val blobServiceClientBuilder: BlobServiceClientBuilder =
        new BlobServiceClientBuilder()
          .endpoint(storageAccountUrl)
          .retryOptions(
            new RequestRetryOptions()
          ) // Sets the default retry options for each request done through the client https://docs.microsoft.com/en-us/java/api/com.azure.storage.common.policy.requestretryoptions.requestretryoptions?view=azure-java-stable#com-azure-storage-common-policy-requestretryoptions-requestretryoptions()
          .httpClient(HttpClient.createDefault())

      // Access the storage using the account key
      if (storageAccountKey != null) {
        blobServiceClientBuilder.credential(
          new StorageSharedKeyCredential(storageAccountName, storageAccountKey)
        )
      }
      // Access the storage using OAuth 2.0 with an Azure service principal
      else if (hc.get(String.format(AccountAuthType, storageAccountName)) == "OAuth") {
        val tenantId = getTenantId(
          new URI(hc.get(String.format(AccountOAuthClientEndpoint, storageAccountName)))
        )
        val clientSecretCredential: ClientSecretCredential = new ClientSecretCredentialBuilder()
          .clientId(hc.get(String.format(AccountOAuthClientId, storageAccountName)))
          .clientSecret(hc.get(String.format(AccountOAuthClientSecret, storageAccountName)))
          .tenantId(tenantId)
          .build()

        blobServiceClientBuilder.credential(clientSecretCredential)
      }

      blobServiceClientBuilder.buildClient
    }
  }

  def apply(storageType: String, hc: Configuration): StorageClient = {
    storageType match {
      case StorageTypeS3 =>
        new S3(hc)
      case StorageTypeAzure =>
        new Azure(hc)
      case _ => throw new IllegalArgumentException("Invalid argument.")
    }
  }

}
