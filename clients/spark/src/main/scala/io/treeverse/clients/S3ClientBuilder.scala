package io.treeverse.clients

import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.PredefinedBackoffStrategies
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID
import scala.util.Try

import io.treeverse.clients.StorageUtils.S3.createAndValidateS3Client

trait S3ClientBuilder extends Serializable {

  /** Name of property from which S3A fetches whether to use path-style S3
   *  access or host-style.
   *
   *  org.apache.hadoop.fs.s3a.Constants defines this property only starting
   *  with version 2.8.  Define it here to support earlier versions.
   */
  protected val S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access"

  /** Return a configured Amazon S3 client similar to the one S3A would use.
   *  On Hadoop versions >=3, S3A can assume a role, and the returned S3
   *  client will similarly assume that role.
   *
   *  @param hc         (partial) Hadoop configuration of fs.s3a.
   *  @param bucket     that this client will access.
   *  @param region     to find this bucket.
   *  @param numRetries number of times to retry on AWS.
   */
  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3
}

object S3ClientBuilder extends S3ClientBuilder {
  val logger: Logger = LoggerFactory.getLogger(getClass.toString + "[hadoop3]")

  def build(hc: Configuration, bucket: String, region: String, numRetries: Int): AmazonS3 = {
    import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
    import com.amazonaws.auth.{
      AWSCredentialsProvider,
      DefaultAWSCredentialsProviderChain,
      STSAssumeRoleSessionCredentialsProvider,
      AWSStaticCredentialsProvider,
      BasicAWSCredentials
    }
    import io.treeverse.clients.LakeFSContext
    import org.apache.hadoop.fs.s3a.Constants

    val minBackoffMsecs = hc.getInt(LakeFSContext.LAKEFS_CONF_GC_S3_MIN_BACKOFF_SECONDS,
                                    LakeFSContext.DEFAULT_LAKEFS_CONF_GC_S3_MIN_BACKOFF_SECONDS
                                   ) * 1000
    val maxBackoffMsecs = hc.getInt(LakeFSContext.LAKEFS_CONF_GC_S3_MAX_BACKOFF_SECONDS,
                                    LakeFSContext.DEFAULT_LAKEFS_CONF_GC_S3_MAX_BACKOFF_SECONDS
                                   ) * 1000

    val backoffStrategy =
      new PredefinedBackoffStrategies.FullJitterBackoffStrategy(minBackoffMsecs, maxBackoffMsecs)
    val retryPolicy =
      new RetryPolicy(new S3RetryDeleteObjectsCondition(), backoffStrategy, numRetries, true)
    val configuration = new ClientConfiguration()
      .withRetryPolicy(retryPolicy)
      .withThrottledRetries(true)
    val s3Endpoint = hc.get(Constants.ENDPOINT, null)

    // TODO(ariels): Support different per-bucket configuration methods.
    //     Possibly pre-generate a FileSystem to access the desired bucket,
    //     and query for its credentials provider.  And cache them, in case
    //     some objects live in different buckets.
    val roleArn = hc.getTrimmed(Constants.ASSUMED_ROLE_ARN, "")
    val accessKey = hc.getTrimmed(Constants.ACCESS_KEY, null)
    val secretKey = hc.getTrimmed(Constants.SECRET_KEY, null)
    val wantHadoopAssume =
      hc.getTrimmed(Constants.AWS_CREDENTIALS_PROVIDER, null) == AssumedRoleCredentialProvider.NAME
    val hadoopAssumeAvailable: Boolean = wantHadoopAssume && Try(
      Class.forName("org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    ).toOption.exists(classOf[AWSCredentialsProvider].isAssignableFrom)
    val useHadoopProvider = wantHadoopAssume && hadoopAssumeAvailable

    val base: AWSCredentialsProvider =
      if (useHadoopProvider) {
        logger.info("Using Hadoop AssumedRoleCredentialProvider as base.")
        new AssumedRoleCredentialProvider(new java.net.URI(s"s3a://$bucket"), hc)
      } else if (accessKey != null && secretKey != null) {
        logger.info("Using access key ID {} {}", accessKey: Any, "secret key ******")
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
      } else {
        logger.info("Using DefaultAWSCredentialsProviderChain")
        new DefaultAWSCredentialsProviderChain()
      }

    val credentialsProvider: AWSCredentialsProvider =
      if (roleArn.nonEmpty && wantHadoopAssume && !hadoopAssumeAvailable) {
        new STSAssumeRoleSessionCredentialsProvider.Builder(
          roleArn,
          s"lakefs-gc-${UUID.randomUUID().toString}"
        )
          .withLongLivedCredentialsProvider(base)
          .build()
      } else {
        base
      }

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
