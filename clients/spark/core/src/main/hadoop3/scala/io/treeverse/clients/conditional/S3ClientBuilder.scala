package io.treeverse.clients.conditional

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.ClientConfiguration
import org.apache.hadoop.conf.Configuration

object S3ClientBuilder extends io.treeverse.clients.S3ClientBuilder {
  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3 = {
    import org.apache.hadoop.fs.s3a.Constants
    import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider

    val configuration = new ClientConfiguration().withMaxErrorRetry(numRetries)

    // TODO(ariels): Support different per-bucket configuration methods.
    //     Possibly pre-generate a FileSystem to access the desired bucket,
    //     and query for its credentials provider.  And cache them, in case
    //     some objects live in different buckets.
    val credentialsProvider =
      if (hc.get(Constants.AWS_CREDENTIALS_PROVIDER) == AssumedRoleCredentialProvider.NAME)
        Some(new AssumedRoleCredentialProvider(new java.net.URI("s3a://" + bucket), hc))
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
