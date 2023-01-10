package io.treeverse.clients

import com.amazonaws.auth.{
  AWSCredentialsProvider,
  AWSStaticCredentialsProvider,
  BasicAWSCredentials
}
import com.amazonaws.services.s3.model.Region
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.http.HttpStatus
import com.amazonaws.{ClientConfiguration, HttpMethod, Protocol, SdkClientException}
import okhttp3.HttpUrl
import okhttp3.mockwebserver.{MockResponse, MockWebServer, RecordedRequest}
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
    it("should create a client after a successful validation") {
      server.enqueue(new MockResponse().setResponseCode(200))
      val initializedClient: AmazonS3 = StorageUtils.S3.createAndValidateS3Client(
        clientConfiguration,
        Some(credentialsProvider),
        awsS3ClientBuilder,
        ENDPOINT,
        US_WEST_2,
        BUCKET_NAME
      )

      server.getRequestCount should equal(1)
      val request: RecordedRequest = server.takeRequest()
      initializedClient should not be null
      initializedClient.getRegion.toString should equal(US_WEST_2)
      extractBucketFromRecordedRequest(request) should equal(BUCKET_NAME)
      request.getMethod should equal(HttpMethod.HEAD.toString)
    }

    it("should create the S3 client successfully with US STANDARD region validation") {
      server.enqueue(new MockResponse().setBody("successful validation response"))
      val initializedClient: AmazonS3 = initializeClient()

      server.getRequestCount should equal(1)

      val request: RecordedRequest = server.takeRequest()
      initializedClient should not be null
      initializedClient.getRegion.name should equal(Region.US_Standard.name())
      extractBucketFromRecordedRequest(request) should equal(BUCKET_NAME)
      request.getMethod should equal(HttpMethod.HEAD.toString)
    }

    it("should get the correct region for the given bucket and create the S3 client successfully") {
      server.enqueue(new MockResponse().setResponseCode(HttpStatus.SC_FORBIDDEN))
      server.enqueue(
        new MockResponse()
          .setBody(generateGetBucketLocationResponseWithRegion(US_WEST_2))
          .setResponseCode(HttpStatus.SC_OK)
      )
      val initializedClient: AmazonS3 = initializeClient()

      server.getRequestCount should equal(2)
      val headBucketRequest: RecordedRequest = server.takeRequest()
      val getLocationRequest: RecordedRequest = server.takeRequest()
      initializedClient should not be null
      initializedClient.getRegion.toString should equal(US_WEST_2)
      extractBucketFromRecordedRequest(headBucketRequest) should equal(BUCKET_NAME)
      headBucketRequest.getMethod should equal(HttpMethod.HEAD.toString)
      getLocationRequest.getMethod should equal(HttpMethod.GET.toString)
    }

    it("should fail creating a client due to failed 'getBucketLocation' request") {
      server.enqueue(new MockResponse().setResponseCode(HttpStatus.SC_FORBIDDEN))
      server.enqueue(
        new MockResponse()
          .setResponseCode(HttpStatus.SC_NOT_FOUND)
      )

      assertThrows[SdkClientException] {
        initializeClient()
      }

      server.getRequestCount should equal(2)
      val headBucketRequest: RecordedRequest = server.takeRequest()
      val getLocationRequest: RecordedRequest = server.takeRequest()
      extractBucketFromRecordedRequest(headBucketRequest) should equal(BUCKET_NAME)
      headBucketRequest.getMethod should equal(HttpMethod.HEAD.toString)
      getLocationRequest.getMethod should equal(HttpMethod.GET.toString)
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
