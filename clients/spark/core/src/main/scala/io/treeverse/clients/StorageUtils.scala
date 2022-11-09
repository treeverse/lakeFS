package io.treeverse.clients

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
  }
}
