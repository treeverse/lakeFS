package io.treeverse.clients.conditional

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.model.{HeadBucketRequest, Region}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.hadoop.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}

object S3ClientBuilder extends io.treeverse.clients.S3ClientBuilder {
  val logger: Logger = LoggerFactory.getLogger(getClass.toString + "[hadoop3]")

  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3 = {
    import org.apache.hadoop.fs.s3a.Constants
    import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
    import com.amazonaws.auth.{BasicAWSCredentials, AWSStaticCredentialsProvider}

    val configuration = new ClientConfiguration().withMaxErrorRetry(numRetries)

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

    initializeClient(configuration, credentialsProvider, region, bucket)
  }

  private def initializeClient(
      config: ClientConfiguration,
      credentialsProvider: Option[AWSCredentialsProvider],
      region: String,
      bucket: String
  ): AmazonS3 = {
    val builder = AmazonS3ClientBuilder
      .standard()
      .withClientConfiguration(config)
      .withRegion(region)
    val builderWithCredentials = credentialsProvider match {
      case Some(cp) => builder.withCredentials(cp)
      case None     => builder
    }

    val client = builderWithCredentials.build

    if (validateClientCorrectness(client, bucket)) {
      return client
    }
    val bucketRegion = getAWSS3Region(client, bucket)
    builder.withRegion(bucketRegion).build
  }

  private def validateClientCorrectness(client: AmazonS3, bucket: String): Boolean = {
    try {
      client.headBucket(new HeadBucketRequest(bucket))
      true
    } catch {
      case _: Exception => false
    }
  }

  private def getAWSS3Region(client: AmazonS3, bucket: String): String = {
    val bucketRegion = client.getBucketLocation(bucket)
    val region = Region.fromValue(bucketRegion)
    // The comparison `region.equals(Region.US_Standard))` is required due to AWS's backward compatibility:
    // https://github.com/aws/aws-sdk-java/issues/1470.
    // "us-east-1" was previously called "US Standard". This resulted in a return value of "US" when
    // calling `client.getBucketLocation(bucket)`.
    if (region.equals(Region.US_Standard)) "us-east-1"
    else region.toString
  }
}
