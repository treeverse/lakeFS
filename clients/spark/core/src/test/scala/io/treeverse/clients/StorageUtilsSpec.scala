package io.treeverse.clients

import com.amazonaws.{ClientConfiguration, Protocol}
import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.thirdparty.apache.http.protocol.HTTP
import org.mockserver.configuration.Configuration
import org.mockserver.integration.ClientAndServer
import org.mockserver.logging.MockServerLogger
import org.mockserver.mock.Expectation
import org.mockserver.model.{HttpRequest, HttpResponse}
import org.mockserver.socket.tls.KeyStoreFactory
import org.mockserver.verify.VerificationTimes

import javax.net.ssl.HttpsURLConnection
//import okhttp3.mockwebserver.{MockResponse, MockWebServer, RecordedRequest}
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class StorageUtilsSpec extends AnyFunSpec with BeforeAndAfter with MockitoSugar with Matchers {
  private val credentialsProvider: AWSCredentialsProvider = new AWSStaticCredentialsProvider(
    new BasicAWSCredentials("ACCESS_KEY", "SECRET_KEY")
  )

  private var awsS3ClientBuilder: AmazonS3ClientBuilder = null
//  private var server: MockWebServer = null
  private var server: ClientAndServer = null
  private var clientConfiguration: ClientConfiguration = null

  private val US_STANDARD = "US"
  private val US_WEST_2 = "us-west-2"
  private val BUCKET_NAME = "bucket"

  before {
//    server = new MockWebServer
    server = ClientAndServer.startClientAndServer()
//    HttpsURLConnection.setDefaultSSLSocketFactory(new KeyStoreFactory(Configuration.configuration(), new MockServerLogger).sslContext.getSocketFactory)
    server.reset
//    server.start()
    awsS3ClientBuilder = AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
//    val baseUrl = server.url("/")
    clientConfiguration = new ClientConfiguration()
      .withProtocol(Protocol.HTTP)
      .withProxyHost("127.0.0.1")
//      .withProxyHost(baseUrl.host())
      .withProxyPort(server.getPort)
//      .withProxyPort(baseUrl.port())
    println("Server port is: " + server.getPort)
  }

  after {
    if(server != null) {
      server.reset()
      server.stop()
    }
//    server.shutdown()
  }

  describe("createAndValidateS3Client") {
    it("should create a client after a successful validation") {
//      server.enqueue(new MockResponse().setResponseCode(200))
      val expectations: Array[Expectation] = server
        .when(
          HttpRequest.request())
        .respond(
          HttpResponse.response()
            .withStatusCode(200)
        )
      val initializedClient: AmazonS3 = StorageUtils.S3.createAndValidateS3Client(
        clientConfiguration,
        Some(credentialsProvider),
        awsS3ClientBuilder,
        US_WEST_2,
        BUCKET_NAME
      )

//      server.getRequestCount should equal (1)
//      val request: RecordedRequest = server.takeRequest()
      initializedClient should not be null
      initializedClient.getRegion.toString should equal (US_WEST_2)
//      extractBucketFromRecordedRequest(request) should equal (BUCKET_NAME)
//      request.getMethod should equal (HttpMethod.HEAD.toString)
      server.verify(HttpRequest.request(), VerificationTimes.once())
      expectations.length should equal (1)
      for ( ex <- expectations ) {
        println(ex.getHttpRequest)
      }
    }

//    it("should create the S3 client successfully with US STANDARD region validation") {
//      server.enqueue(new MockResponse().setBody("successful validation response"))
//      val initializedClient: AmazonS3 = StorageUtils.S3.createAndValidateS3Client(
//        clientConfiguration,
//        Some(credentialsProvider),
//        awsS3ClientBuilder,
//        US_STANDARD,
//        BUCKET_NAME
//      )
//
//      server.getRequestCount should equal(1)
//
//      val request: RecordedRequest = server.takeRequest()
//      initializedClient should not be null
//      initializedClient.getRegion.name should equal(Region.US_Standard.name())
//      extractBucketFromRecordedRequest(request) should equal(BUCKET_NAME)
//      request.getMethod should equal(HttpMethod.HEAD.toString)
//    }
//
//    it("should get the correct region for the given bucket and create the S3 client successfully") {
//      server.enqueue(new MockResponse().setBody("HeadBucket- Out of luck").setResponseCode(HttpStatus.SC_FORBIDDEN))
//      server.enqueue(new MockResponse()
//        .setBody(generateGetBucketLocationResponseWithRegion(US_WEST_2))
//      )
//      val initializedClient: AmazonS3 = StorageUtils.S3.createAndValidateS3Client(
//        clientConfiguration,
//        Some(credentialsProvider),
//        awsS3ClientBuilder,
//        US_STANDARD,
//        BUCKET_NAME
//      )
//
//
//      server.getRequestCount should equal(2)
//      val headBucketRequest: RecordedRequest = server.takeRequest()
//      val getLocationRequest: RecordedRequest = server.takeRequest()
//      initializedClient should not be null
//      initializedClient.getRegion.toString should equal(US_WEST_2)
//      extractBucketFromRecordedRequest(headBucketRequest) should equal(BUCKET_NAME)
//      headBucketRequest.getMethod should equal(HttpMethod.HEAD.toString)
//      getLocationRequest.getMethod should equal(HttpMethod.GET.toString)
//      println("getBucketLocation: " + getLocationRequest.getRequestLine)
//    }

    it("should fail creating a client due to failed 'getBucketLocation' request") {

    }

    it("should fail creating a client due to failed 'headBucket' request") {

    }
  }

//  private def extractBucketFromRecordedRequest(request: RecordedRequest): String = {
//    val splicedRequestLine = request.getRequestLine.split('/')
//    println(splicedRequestLine.length)
//    splicedRequestLine(splicedRequestLine.length-3)
//  }

  private def generateGetBucketLocationResponseWithRegion(region: String): String ={
    s"""<?xml version="1.0" encoding="UTF-8"?>\n<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">$region</LocationConstraint>"""
  }
}
