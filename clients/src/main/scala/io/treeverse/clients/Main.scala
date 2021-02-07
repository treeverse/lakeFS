package io.treeverse.clients

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.treeverse.clients.s3.{S3MetaRangeReader, S3RangeReader}

object Main {
  val REGION = "us-east-1"
  val PROFILE = "yoni"

  def main(args: Array[String]): Unit = {
    val metaRangeID = "c6e1391c3677c56119a62827939ebafe4b812200039ddd490f2b5625ca105b5c"
    val repoPrefix = "javajava"
    val s3 = AmazonS3ClientBuilder.standard().withRegion(REGION).withCredentials(new ProfileCredentialsProvider(PROFILE)).build()
    val metaRangeReader = new S3MetaRangeReader(s3, "yoni-test3", repoPrefix)
    val rangeReader = new S3RangeReader(s3, "yoni-test3", repoPrefix)
    val ranges = metaRangeReader.getRanges(metaRangeID)
    for (r <- ranges) {
      val records = rangeReader.getEntries(r.identity)
      for (rec <- records) {
        println(rec.key + " = " + rec.identity + "\n\t" + rec.entry.address);
      }
    }
  }
}