package io.treeverse.clients.conditional

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.ClientConfiguration
import org.apache.hadoop.conf.Configuration

object S3ClientBuilder extends io.treeverse.clients.S3ClientBuilder {
  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3 = {
    import org.apache.hadoop.fs.s3a.Constants
    import com.amazonaws.auth.{BasicAWSCredentials, AWSStaticCredentialsProvider}

    val configuration = new ClientConfiguration().withMaxErrorRetry(numRetries)

    val credentialsProvider =
      if (hc.get(Constants.ACCESS_KEY) != null)
        Some(
          new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(hc.get(Constants.ACCESS_KEY), hc.get(Constants.SECRET_KEY))
          )
        )
      else None

    val builder = AmazonS3ClientBuilder
      .standard()
      .withClientConfiguration(configuration)
      .withRegion(region)

    val builderWithCredentials = credentialsProvider match {
      case Some(cp) => builder.withCredentials(cp)
      case None     => builder
    }

    builderWithCredentials.build
  }
}
