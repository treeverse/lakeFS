package io.treeverse.clients

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.thirdparty.apache.http.HttpStatus
import okhttp3.HttpUrl
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class StorageUtilsSpec extends AnyFunSpec with BeforeAndAfter with MockitoSugar with Matchers {
  private val credentialsProvider: AWSCredentialsProvider = new AWSStaticCredentialsProvider(
    new BasicAWSCredentials("ACCESS_KEY", "SECRET_KEY")
  )

  private val awsS3ClientBuilder: AmazonS3ClientBuilder =
    AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
  private var server: MockWebServer = null
  private var clientConfiguration: ClientConfiguration = null

  private val ENDPOINT = "http://s3.example.net"
  private val US_STANDARD = "US"
  private val US_WEST_2 = "us-west-2"
  private val AP_SOUTHEAST_1 = "ap-southeast-1"
  private val BUCKET_NAME = "bucket"

  before {
    server = new MockWebServer
    server.start()
    clientConfiguration = generateS3ClientConfigurations(server.url("/"))
  }

  after {
    if (server != null) {
      server.shutdown()
    }
  }

  describe("createAndValidateS3Client") {
    it("should create a client after fetching the region") {
      server.enqueue(
        new MockResponse()
          .setBody(generateGetBucketLocationResponseWithRegion(US_WEST_2))
          .setResponseCode(HttpStatus.SC_OK)
      )
      val initializedClient: AmazonS3 = StorageUtils.S3.createAndValidateS3Client(
        clientConfiguration,
        Some(credentialsProvider),
        awsS3ClientBuilder,
        ENDPOINT,
        US_WEST_2,
        BUCKET_NAME
      )

      server.getRequestCount should equal(2)
      val request: RecordedRequest = server.takeRequest()
      initializedClient should not be null
      initializedClient.getRegion.toString should equal(US_WEST_2)
      extractBucketFromRecordedRequest(request) should equal(BUCKET_NAME)
    }
    it(
      "should create the client if the provided region is different from the bucket region"
    ) {
      server.enqueue(
        new MockResponse()
          .setBody(generateGetBucketLocationResponseWithRegion(US_WEST_2))
          .setResponseCode(HttpStatus.SC_OK)
      )
      val initializedClient: AmazonS3 = StorageUtils.S3.createAndValidateS3Client(
        clientConfiguration,
        Some(credentialsProvider),
        awsS3ClientBuilder,
        ENDPOINT,
        AP_SOUTHEAST_1,
        BUCKET_NAME
      )

      server.getRequestCount should equal(2)
      val request: RecordedRequest = server.takeRequest()
      initializedClient should not be null
      initializedClient.getRegion.toString should equal(US_WEST_2)
      extractBucketFromRecordedRequest(request) should equal(BUCKET_NAME)
    }
    it(
      "should create the client if the provided region is different from the bucket region (US_STANDARD)"
    ) {
      server.enqueue(
        new MockResponse()
          .setBody(
            generateGetBucketLocationResponseWithRegion("")
          ) // buckets on us-east-1 return an empty string here
          .setResponseCode(HttpStatus.SC_OK)
      )
      val initializedClient: AmazonS3 = StorageUtils.S3.createAndValidateS3Client(
        clientConfiguration,
        Some(credentialsProvider),
        awsS3ClientBuilder,
        ENDPOINT,
        US_WEST_2,
        BUCKET_NAME
      )

      server.getRequestCount should equal(2)
      val request: RecordedRequest = server.takeRequest()
      initializedClient should not be null
      initializedClient.getRegion.toString should be(null)
      extractBucketFromRecordedRequest(request) should equal(BUCKET_NAME)
    }

    it("should use provided region is failed to fetch region") {
      server.enqueue(
        new MockResponse()
          .setBody("failed to fetch region")
          .setResponseCode(HttpStatus.SC_FORBIDDEN)
      )
      val initializedClient: AmazonS3 = StorageUtils.S3.createAndValidateS3Client(
        clientConfiguration,
        Some(credentialsProvider),
        awsS3ClientBuilder,
        ENDPOINT,
        US_WEST_2,
        BUCKET_NAME
      )
      server.getRequestCount should equal(1)
      val getLocationRequest: RecordedRequest = server.takeRequest()
      initializedClient should not be null
      initializedClient.getRegion.toString should equal(US_WEST_2)
      extractBucketFromRecordedRequest(getLocationRequest) should equal(BUCKET_NAME)
    }
  }

  describe("concatKeysToStorageNamespace") {
    val keys = Seq("k1")

    it("should keep namespace scheme and host and namespace trailing slash") {
      val storageNSWithPath = "s3://bucket/foo/"
      validateConcatKeysToStorageNamespace(keys,
                                           storageNSWithPath,
                                           true,
                                           Seq("s3://bucket/foo/k1")
                                          ) should equal(true)

      val storageNSWithoutPath = "s3://bucket/"
      validateConcatKeysToStorageNamespace(keys,
                                           storageNSWithoutPath,
                                           true,
                                           Seq("s3://bucket/k1")
                                          ) should equal(true)
    }

    it("should keep namespace scheme and host and add namespace trailing slash") {
      val storageNSWithPath = "s3://bucket/foo"
      validateConcatKeysToStorageNamespace(keys,
                                           storageNSWithPath,
                                           true,
                                           Seq("s3://bucket/foo/k1")
                                          ) should equal(true)

      val storageNSWithoutPath = "s3://bucket"
      validateConcatKeysToStorageNamespace(keys,
                                           storageNSWithoutPath,
                                           true,
                                           Seq("s3://bucket/k1")
                                          ) should equal(true)
    }

    it("should drop namespace scheme and host and keep namespace trailing slash") {
      val storageNSWithPath = "s3://bucket/foo/"
      validateConcatKeysToStorageNamespace(keys,
                                           storageNSWithPath,
                                           false,
                                           Seq("foo/k1")
                                          ) should equal(true)

      val storageNSWithoutPath = "s3://bucket/"
      validateConcatKeysToStorageNamespace(keys,
                                           storageNSWithoutPath,
                                           false,
                                           Seq("k1")
                                          ) should equal(true)
    }

    it("should drop namespace scheme and host and add namespace trailing slash") {
      val storageNSWithPath = "s3://bucket/foo"
      validateConcatKeysToStorageNamespace(keys,
                                           storageNSWithPath,
                                           false,
                                           Seq("foo/k1")
                                          ) should equal(true)

      val storageNSWithoutPath = "s3://bucket"
      validateConcatKeysToStorageNamespace(keys,
                                           storageNSWithoutPath,
                                           false,
                                           Seq("k1")
                                          ) should equal(true)
    }
  }

  private def extractBucketFromRecordedRequest(request: RecordedRequest): String = {
    val splitRequestLine = request.getRequestLine.split('/')
    if (splitRequestLine.length < 3) {
      return ""
    }
    splitRequestLine(splitRequestLine.length - 3)
  }

  private def generateGetBucketLocationResponseWithRegion(region: String): String = {
    s"""<?xml version="1.0" encoding="UTF-8"?>\n<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">$region</LocationConstraint>"""
  }

  private def generateS3ClientConfigurations(baseUrl: HttpUrl): ClientConfiguration = {
    new ClientConfiguration()
      .withProxyHost(baseUrl.host())
      .withProxyPort(baseUrl.port())
      .withProtocol(Protocol.HTTP)
      .withMaxErrorRetry(0)
      .withSocketTimeout(15000)
      .withConnectionTimeout(15000)
  }

  private def initializeClient(): AmazonS3 = {
    StorageUtils.S3.createAndValidateS3Client(
      clientConfiguration,
      Some(credentialsProvider),
      awsS3ClientBuilder,
      ENDPOINT,
      US_STANDARD,
      BUCKET_NAME
    )
  }

  private def validateConcatKeysToStorageNamespace(
      keys: Seq[String],
      storageNamespace: String,
      keepNsSchemeAndHost: Boolean,
      expectedResult: Seq[String]
  ): Boolean = {
    val res = StorageUtils.concatKeysToStorageNamespace(keys, storageNamespace, keepNsSchemeAndHost)
    res.toSet == expectedResult.toSet
  }
}
