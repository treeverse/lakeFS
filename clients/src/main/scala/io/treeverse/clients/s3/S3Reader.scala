package io.treeverse.clients.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import com.google.protobuf.Message
import io.treeverse.clients.{EntryRecord, ProtoReader, SSTableReader}

import java.io.File

class S3Reader[Proto <: Message](val s3: AmazonS3, val bucket: String, val repositoryPrefix: String, val typeName: String, messagePrototype: Proto) extends ProtoReader[Proto] {
  val reader = new SSTableReader

  override def get(rangeID: Array[Byte]): Seq[EntryRecord[Proto]] = {
    val localFile = File.createTempFile("lakefs", typeName)
    // TODO(yoni): When reading lazily "on exit" is too soon to delete.  Need a proper
    //     closeable iterator.
    localFile.deleteOnExit()
    s3.getObject(new GetObjectRequest(bucket, "%s/_lakefs/%s".format(repositoryPrefix, new String(rangeID))), localFile)
    try {
      reader.get(localFile.getAbsolutePath, messagePrototype, typeName)
    }
    finally {
      localFile.delete()
    }
  }
}
