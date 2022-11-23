package io.treeverse.clients.conditional

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import io.treeverse.clients.StorageUtils.S3.createAndValidateS3Client
import org.apache.hadoop.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}

object S3ClientBuilder extends io.treeverse.clients.S3ClientBuilder {
  val logger: Logger = LoggerFactory.getLogger(getClass.toString + "[hadoop2]")

  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3 = {
    import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
    import org.apache.hadoop.fs.s3a.Constants

    val configuration = new ClientConfiguration().withMaxErrorRetry(numRetries)

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

    createAndValidateS3Client(configuration,
                              credentialsProvider,
                              AmazonS3ClientBuilder.standard(),
                              region,
                              bucket
                             )
  }
}
