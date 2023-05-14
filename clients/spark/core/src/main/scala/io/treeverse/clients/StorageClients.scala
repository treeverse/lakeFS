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

trait StorageClient {
}

object StorageClients {
  class S3() extends StorageClient with Serializable {
    private var _retries: Int = 0
    private var _region: String = "us-east-1"
    private var _bucket: String = ""
    private var _storageNSURI: URI = _
    private var _config: ConfigMapper = _
    @transient private var _s3Client: AmazonS3 = _
    def this(
        configMapper: ConfigMapper,
        storageNamespace: String,
        region: String,
        numRetries: Int
    ) = {
      this()
      _storageNSURI = new URI(storageNamespace)
      _bucket = _storageNSURI.getHost
      _config = configMapper
      _region = region
      _retries = numRetries
    }

    def s3Client(): AmazonS3 = {
      if (_s3Client == null) {
        _s3Client = io.treeverse.clients.conditional.S3ClientBuilder
          .build(_config.configuration, _bucket, _region, _retries)
      }
      _s3Client
    }
  }

  class Azure() extends StorageClient with Serializable {
    private var _containerName: String = ""
    private var _storageAccountUrl: String = ""
    private var _storageAccountName: String = ""
    private var _config: ConfigMapper = _
    private var _storageNSURI: URI = _
    @transient private var _blobContainerClient: BlobContainerClient = _
    @transient private var _blobServiceClient: BlobServiceClient = _
    @transient private var _blobBatchClient: BlobBatchClient = _

    def this(configMapper: ConfigMapper, storageNamespace: String) = {
      this()
      _storageNSURI = new URI(storageNamespace)
      val uri = new URI(storageNamespace)
      _config = configMapper
      _storageAccountUrl = StorageUtils.AzureBlob.uriToStorageAccountUrl(uri)
      _storageAccountName = StorageUtils.AzureBlob.uriToStorageAccountName(uri)
      _containerName = StorageUtils.AzureBlob.uriToContainerName(uri)
    }

    private def blobServiceClient(): BlobServiceClient = {
      if (_blobServiceClient == null) {
        _blobServiceClient = getBlobServiceClient(_storageAccountUrl, _storageAccountName, _config)
      }
      _blobServiceClient
    }

    def blobBatchClient(): BlobBatchClient = {
      if (_blobBatchClient == null) {
        _blobBatchClient = new BlobBatchClientBuilder(blobServiceClient()).buildClient
      }
      _blobBatchClient
    }

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
        new S3(configMapper, storageNamespace, region, S3.S3NumRetries)
      case StorageTypeAzure =>
        new Azure(configMapper, storageNamespace)
      case _ => throw new IllegalArgumentException("Invalid argument.")
    }
  }
}
