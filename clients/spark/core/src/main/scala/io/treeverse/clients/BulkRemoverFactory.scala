package io.treeverse.clients

import com.amazonaws.services.s3.model
import com.amazonaws.services.s3.model.MultiObjectDeleteException
import com.azure.core.http.rest.Response
import com.azure.storage.blob.models.DeleteSnapshotsOptionType
import io.treeverse.clients.StorageClients.{Azure, S3}
import io.treeverse.clients.StorageUtils.AzureBlob._
import io.treeverse.clients.StorageUtils.S3._

import java.net.URI
import java.nio.charset.Charset
import java.util.stream.Collectors
import scala.collection.JavaConverters._

trait BulkRemover {

  /** Provides the max bulk size allowed by the underlying SDK client that does the actual deletion.
   *
   *  @return max bulk size
   */
  def getMaxBulkSize(): Int

  /** Constructs object URIs so that they are consumable by the SDK client that does the actual deletion.
   *
   *  @param keys keys of objects to be removed
   *  @param storageNamespace the storage namespace in which the objects are stored
   *  @param keepNsSchemeAndHost whether to keep a storage namespace of the form "s3://bucket/foo/" or remove its URI
   *                            scheme and host leaving it in the form "/foo/"
   *  @param applyUTF8Encoding whether to UTF-8 encode keys
   *  @return object URIs of the keys
   */
  def constructRemoveKeyNames(
      keys: Seq[String],
      storageNamespace: String,
      keepNsSchemeAndHost: Boolean,
      applyUTF8Encoding: Boolean
  ): Seq[String] = {
    println("storageNamespace: " + storageNamespace)
    var removeKeyNames =
      StorageUtils.concatKeysToStorageNamespace(keys, storageNamespace, keepNsSchemeAndHost)
    if (applyUTF8Encoding) {
      removeKeyNames = removeKeyNames
        .map(x => x.getBytes(Charset.forName("UTF-8")))
        .map(x => new String(x))
    }
    removeKeyNames
  }

  /** Bulk delete objects from the underlying storage.
   *
   *  @param keys of objects to delete
   *  @param storageNamespace the storage namespace in which the objects are stored
   *  @return objects that removed successfully
   */
  def deleteObjects(keys: Seq[String], storageNamespace: String): Seq[String]
}

object BulkRemoverFactory {
  private class S3BulkRemover(
      storageNamespace: String,
      client: StorageClients.S3
  ) extends BulkRemover {
    import scala.collection.JavaConversions._

    private val uri = new URI(storageNamespace)
    private val bucket = uri.getHost

    override def deleteObjects(keys: Seq[String], storageNamespace: String): Seq[String] = {
      val removeKeyNames = constructRemoveKeyNames(keys,
                                                   storageNamespace,
                                                   keepNsSchemeAndHost = false,
                                                   applyUTF8Encoding = false
                                                  )
      println(s"Remove keys from ${bucket}: ${removeKeyNames.take(100).mkString(", ")}")
      val removeKeys = removeKeyNames.map(k => new model.DeleteObjectsRequest.KeyVersion(k)).asJava

      val delObjReq = new model.DeleteObjectsRequest(bucket).withKeys(removeKeys)
      val s3Client = client.s3Client
      try {
        val res = s3Client.deleteObjects(delObjReq)
        res.getDeletedObjects.asScala.map(_.getKey())
      } catch {
        case mde: MultiObjectDeleteException => {
          // TODO(ariels): Delete one-by-one?!

          // TODO(ariels): Metric!
          val errors = mde.getErrors();
          println(s"deleteObjects: Partial failure: ${errors.size} errors: ${errors}")
          errors.foreach((de) => println(s"\t${de.getKey}: [${de.getCode}] ${de.getMessage}"))
          mde.getDeletedObjects.asScala.map(_.getKey)
        }
        case e: Exception => {
          println(s"deleteObjects failed: ${e}")
          throw e
        }
      }
    }

    override def getMaxBulkSize(): Int = {
      S3MaxBulkSize
    }
  }

  private class AzureBlobBulkRemover(
      client: StorageClients.Azure
  ) extends BulkRemover {

    override def deleteObjects(keys: Seq[String], storageNamespace: String): Seq[String] = {
      val removeKeyNames = constructRemoveKeyNames(keys, storageNamespace, true, true)
      println(s"Remove keys: ${removeKeyNames.take(100).mkString(", ")}")
      val removeKeys = removeKeyNames.asJava
      val blobBatchClient = client.blobBatchClient

      val extractUrlIfBlobDeleted = new java.util.function.Function[Response[Void], URI]() {
        def apply(response: Response[Void]): URI = {
          if (response.getStatusCode == 200) {
            response.getRequest.getUrl
          }
          new URI("")
        }
      }
      val uriToString = new java.util.function.Function[URI, String]() {
        def apply(uri: URI): String = uri.toString
      }
      val isNonEmptyString = new java.util.function.Predicate[String]() {
        override def test(s: String): Boolean = s.nonEmpty
      }

      try {
        val responses = blobBatchClient.deleteBlobs(removeKeys, DeleteSnapshotsOptionType.INCLUDE)
        // TODO(Tals): extract uris of successfully deleted objects from response, the current version does not do that.
        responses
          .stream()
          .map[URI](extractUrlIfBlobDeleted)
          .map[String](uriToString)
          .filter(isNonEmptyString)
          .collect(Collectors.toList())
          .asScala
          .toSeq
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          Seq.empty
      }
    }

    override def getMaxBulkSize(): Int = {
      AzureBlobMaxBulkSize
    }
  }

  def apply(storageClient: StorageClient, storageNamespace: String): BulkRemover = {
    storageClient match {
      case s3: S3 =>
        new S3BulkRemover(storageNamespace, s3)
      case azure: Azure =>
        new AzureBlobBulkRemover(azure)
      case _ => throw new IllegalArgumentException("Invalid argument.")
    }
  }
}
