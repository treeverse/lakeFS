package io.treeverse.clients

import com.amazonaws.services.s3.AmazonS3
import com.azure.core.http.HttpClient
import com.azure.identity.{ClientSecretCredential, ClientSecretCredentialBuilder}
import com.azure.storage.blob.batch.{BlobBatchClient, BlobBatchClientBuilder}
import com.azure.storage.blob.{BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.RequestRetryOptions
import io.treeverse.clients.StorageUtils.AzureBlob._
import org.apache.hadoop.conf.Configuration

import java.net.URI

object StorageClients {

  object S3 {
    def getS3Client(
        hc: Configuration,
        bucket: String,
        region: String,
        numRetries: Int
    ): AmazonS3 =
      io.treeverse.clients.conditional.S3ClientBuilder.build(hc, bucket, region, numRetries)
  }

  object Azure {

    def getBlobContainerClient(
        hc: Configuration,
        storageAccountUrl: String,
        storageAccountName: String,
        containerName: String
    ): BlobContainerClient = {
      val blobServiceClient: BlobServiceClient =
        getBlobServiceClient(hc, storageAccountUrl, storageAccountName)
      blobServiceClient.getBlobContainerClient(containerName)
    }
    def getBlobBatchClient(
        hc: Configuration,
        storageAccountUrl: String,
        storageAccountName: String
    ): BlobBatchClient = {
      val blobServiceClient: BlobServiceClient =
        getBlobServiceClient(hc, storageAccountUrl, storageAccountName)
      new BlobBatchClientBuilder(blobServiceClient).buildClient
    }
  }

  private def getBlobServiceClient(
      hc: Configuration,
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
