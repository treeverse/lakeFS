package io.treeverse.clients.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import io.treeverse.clients.{EntryRecord, RangeReader, SSTableReader}

import java.io.File

class S3RangeReader(var s3: AmazonS3, var bucket: String, var repositoryPrefix: String) extends RangeReader {
  val reader = new SSTableReader

  override def getEntries(rangeID: Array[Byte]): Seq[EntryRecord] = {
    val rangeFile = File.createTempFile("lakefs", "metarange")
    rangeFile.deleteOnExit()
    s3.getObject(new GetObjectRequest(bucket, "%s/_lakefs/%s".format(repositoryPrefix, new String(rangeID))), rangeFile)
    try {
      reader.getEntries(rangeFile.getAbsolutePath)
    }
    finally {
      rangeFile.delete()
    }
  }
}
