package io.treeverse.clients

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import io.treeverse.clients.SSTableReader

import java.io.File

object Main {
  val REGION = "us-east-1"
  val PROFILE = "yoni"

  def main(args: Array[String]): Unit = {
    var metaRangeID = "c6e1391c3677c56119a62827939ebafe4b812200039ddd490f2b5625ca105b5c"
    var s3Client = AmazonS3ClientBuilder.standard().withRegion(REGION).withCredentials(new ProfileCredentialsProvider(PROFILE)).build()
    var metarangeFile = File.createTempFile("lakefs", "metarange")
    metarangeFile.deleteOnExit()
    var obj = s3Client.getObject(new GetObjectRequest("yoni-test3", "javajava/_lakefs/c6e1391c3677c56119a62827939ebafe4b812200039ddd490f2b5625ca105b5c"), metarangeFile)
    var reader = new SSTableReader
    var ranges = reader.getRanges(metarangeFile.getAbsolutePath)
    for (r <- ranges) {
      val rangeFile = File.createTempFile("lakefs", "metarange")
      rangeFile.deleteOnExit()
      obj = s3Client.getObject(new GetObjectRequest("yoni-test3", String.format("javajava/_lakefs/%s", r.identity)), rangeFile)
      var records = reader.getEntries(rangeFile.getAbsolutePath)
      for (rec <- records) {
        println(rec.key + " = " + rec.identity + "\n\t" + rec.entry);
      }
      rangeFile.delete();
    }
  }

}