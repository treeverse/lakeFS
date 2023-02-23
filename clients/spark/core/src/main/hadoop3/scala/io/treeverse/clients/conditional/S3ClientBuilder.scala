package io.treeverse.clients.conditional

import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import io.treeverse.clients.S3RetryCondition
import io.treeverse.clients.StorageUtils.S3.createAndValidateS3Client
import org.apache.hadoop.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}

object S3ClientBuilder extends io.treeverse.clients.S3ClientBuilder {
  val logger: Logger = LoggerFactory.getLogger(getClass.toString + "[hadoop3]")

  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3 = {
    import org.apache.hadoop.fs.s3a.Constants
    import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
    import com.amazonaws.auth.{BasicAWSCredentials, AWSStaticCredentialsProvider}

    val retryPolicy = new RetryPolicy(new S3RetryCondition(), null, numRetries, true)
    val configuration = new ClientConfiguration().withRetryPolicy(retryPolicy)
    val s3Endpoint = hc.get(Constants.ENDPOINT, null)

    // TODO(ariels): Support different per-bucket configuration methods.
    //     Possibly pre-generate a FileSystem to access the desired bucket,
    //     and query for its credentials provider.  And cache them, in case
    //     some objects live in different buckets.
    val credentialsProvider =
      if (hc.get(Constants.AWS_CREDENTIALS_PROVIDER) == AssumedRoleCredentialProvider.NAME) {
        logger.info("Use configured AssumedRoleCredentialProvider for bucket {}", bucket)
        Some(new AssumedRoleCredentialProvider(new java.net.URI("s3a://" + bucket), hc))
      } else if (hc.get(Constants.ACCESS_KEY) != null) {
        logger.info(
          "Use access key ID {} {}",
          hc.get(Constants.ACCESS_KEY): Any,
          if (hc.get(Constants.SECRET_KEY) == null) "(missing secret key)" else "secret key ******"
        )
        Some(
          new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(hc.get(Constants.ACCESS_KEY), hc.get(Constants.SECRET_KEY))
          )
        )
      } else None

    val builder = AmazonS3ClientBuilder
      .standard()
      .withPathStyleAccessEnabled(hc.getBoolean(S3A_PATH_STYLE_ACCESS, true))

    createAndValidateS3Client(configuration,
                              credentialsProvider,
                              builder,
                              s3Endpoint,
                              region,
                              bucket
                             )
  }
}
