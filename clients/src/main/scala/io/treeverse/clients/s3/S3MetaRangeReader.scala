package io.treeverse.clients.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import io.treeverse.clients
import io.treeverse.clients.{MetaRangeReader, SSTableReader}

import java.io.File

class S3MetaRangeReader(var s3: AmazonS3, var bucket: String, var repositoryPrefix: String) extends MetaRangeReader {
  val reader = new SSTableReader
  override def getRanges(metaRangeID: String): List[clients.Range] = {
    val metaRangeFile = File.createTempFile("lakefs", "metarange")
    s3.getObject(new GetObjectRequest(bucket, "%s/_lakefs/%s".format(repositoryPrefix, metaRangeID)), metaRangeFile)
    metaRangeFile.deleteOnExit()
    try {
      reader.getRanges(metaRangeFile.getAbsolutePath)
    } finally {
      metaRangeFile.delete()
    }
  }
}
