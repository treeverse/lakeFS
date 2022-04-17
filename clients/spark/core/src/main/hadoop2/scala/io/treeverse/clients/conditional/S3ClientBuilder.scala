package io.treeverse.clients.conditional

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.ClientConfiguration
import org.apache.hadoop.conf.Configuration

object S3ClientBuilder extends io.treeverse.clients.S3ClientBuilder {
  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3 = {
    val configuration = new ClientConfiguration().withMaxErrorRetry(numRetries)

    val builder = AmazonS3ClientBuilder
      .standard()
      .withClientConfiguration(configuration)
      .withRegion(region)
    builder.build
  }
}
