package io.treeverse.clients.conditional

import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.{PredefinedBackoffStrategies, RetryPolicy}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import io.treeverse.clients.S3RetryDeleteObjectsCondition
import io.treeverse.clients.StorageUtils.S3.createAndValidateS3Client
import org.apache.hadoop.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}

object S3ClientBuilder extends io.treeverse.clients.S3ClientBuilder {
  val logger: Logger = LoggerFactory.getLogger(getClass.toString + "[hadoop2]")

  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3 = {
    import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
    import org.apache.hadoop.fs.s3a.Constants

    val backoffStrategy = new PredefinedBackoffStrategies.FullJitterBackoffStrategy(1000, 120000)
    val retryPolicy = new RetryPolicy(new S3RetryDeleteObjectsCondition(), backoffStrategy, numRetries, true)
    val configuration = new ClientConfiguration()
      .withRetryPolicy(retryPolicy)
      .withThrottleRetries(true)
    val s3Endpoint = hc.get(Constants.ENDPOINT, null)

    val credentialsProvider =
      if (hc.get(Constants.ACCESS_KEY) != null) {
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
