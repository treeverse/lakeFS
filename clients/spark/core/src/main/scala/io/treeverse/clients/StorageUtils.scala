package io.treeverse.clients

import java.net.URI

object StorageUtils {
  val StorageTypeS3 = "s3"
  val StorageTypeAzure = "azure"

  object AzureBlob {
    val StorageAccountKeyPropertyPattern =
      "fs.azure.account.key.<storageAccountName>.dfs.core.windows.net"
    val StorageAccNamePlaceHolder = "<storageAccountName>"
    // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/storage.blobs.batch-readme#key-concepts
    // Note that there is no official java SDK documentation of the max batch size, therefore assuming the above.
    val AzureBlobMaxBulkSize = 256

    /** Converts storage namespace URIs of the form https://<storageAccountName>.blob.core.windows.net/<container>/<path-in-container>
     *  to storage account URL of the form https://<storageAccountName>.blob.core.windows.net and storage namespace format is
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
  }
}
