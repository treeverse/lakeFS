package io.treeverse.clients

import com.amazonaws.services.s3.AmazonS3
import com.azure.core.http.HttpClient
import com.azure.identity.{ClientSecretCredential, ClientSecretCredentialBuilder}
import com.azure.storage.blob.batch.{BlobBatchClient, BlobBatchClientBuilder}
import com.azure.storage.blob.{BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.RequestRetryOptions
import io.treeverse.clients.StorageUtils.AzureBlob._
import io.treeverse.clients.StorageUtils.{S3, StorageTypeAzure, StorageTypeS3}

import java.net.URI

trait StorageClient {}

object StorageClients {
  class S3(storageNamespace: String, region: String, retries: Int, config: ConfigMapper)
      extends StorageClient
      with Serializable {
    private val storageNSURI: URI = new URI(storageNamespace)
    private val bucket: String = storageNSURI.getHost
    @transient lazy val s3Client: AmazonS3 = io.treeverse.clients.conditional.S3ClientBuilder
      .build(config.configuration, bucket, region, retries)
  }

  class Azure(config: ConfigMapper, storageNamespace: String)
      extends StorageClient
      with Serializable {
    private val storageNSURI: URI = new URI(storageNamespace)
    private val storageAccountUrl: String =
      StorageUtils.AzureBlob.uriToStorageAccountUrl(storageNSURI)
    private val storageAccountName: String =
      StorageUtils.AzureBlob.uriToStorageAccountName(storageNSURI)
    @transient private lazy val blobServiceClient: BlobServiceClient =
      getBlobServiceClient(storageAccountUrl, storageAccountName, config)
    @transient lazy val blobBatchClient: BlobBatchClient = new BlobBatchClientBuilder(
      blobServiceClient
    ).buildClient

    private def getBlobServiceClient(
        storageAccountUrl: String,
        storageAccountName: String,
        configMapper: ConfigMapper
    ): BlobServiceClient = {
      val hc = configMapper.configuration
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

  def apply(
      storageType: String,
      configMapper: ConfigMapper,
      storageNamespace: String,
      region: String
  ): StorageClient = {
    storageType match {
      case StorageTypeS3 =>
        new S3(storageNamespace, region, S3.S3NumRetries, configMapper)
      case StorageTypeAzure =>
        new Azure(configMapper, storageNamespace)
      case _ => throw new IllegalArgumentException("Invalid argument.")
    }
  }
}
