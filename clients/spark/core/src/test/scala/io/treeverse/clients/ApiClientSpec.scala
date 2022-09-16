package io.treeverse.clients

import dev.failsafe.FailsafeException

import java.net.{SocketException, URI}
import org.scalatest._
import matchers.should._
import funspec._
import io.lakefs.clients.api.ApiException
import org.mockito.Mockito.{doThrow, spy, times, verify}

class ApiClientSpec extends AnyFunSpec with Matchers {
  describe("translateURI") {
    describe("s3") {
      val translate = (u: String) => ApiClient.translateURI(new URI(u), StorageUtils.StorageTypeS3).toString
      it("should translate to s3a") {
        translate("s3://bucket/path/to/object") should be("s3a://bucket/path/to/object")
      }

      it("should translate unadorned bucket to a bucket with no slashes") {
        translate("s3://bucket") should be("s3a://bucket")
      }

      it("should translate unadorned bucket with a slash to a bucket with a slash") {
        translate("s3://bucket/") should be("s3a://bucket/")
      }

      it("should translate bucket with path ending in a slash to a bucket with a path ending in a slash") {
        translate("s3://bucket/path/") should be("s3a://bucket/path/")
      }

      it("should preserve multiple slashes") {
        translate("s3://two//three///") should be("s3a://two//three///")
      }
    }

    describe("Azure") {
      val translate = (u: String) => ApiClient.translateURI(new URI(u), StorageUtils.StorageTypeAzure).toString
      it("should translate any protocol to abfs") {
        translate("https://account.example.net/container/path/to/blob") should be("abfs://container@account.dfs.core.windows.net/path/to/blob")
        translate("ex://account.example.net/container/path/to/blob") should be("abfs://container@account.dfs.core.windows.net/path/to/blob")
      }

      it("should handle empty paths") {
        // TODO(lynn): Trailing slashes might well be incorrect here.
        translate("https://account.example.net/container/") should be("abfs://container@account.dfs.core.windows.net/")
        translate("https://account.example.net/container") should be("abfs://container@account.dfs.core.windows.net/")
      }
    }
  }
}

class RequestRetryWrapperSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {
  val SomeValue = "some value"
  val DefaultMaxNumRetries = 5
  val DefaultMaxNumAttempts = 6

  var retryWrapper = spy(new RequestRetryWrapper)
  var dummyMethodInvoker = spy(new DummyMethodInvoker)

  override def beforeEach(): Unit = {
    super.beforeEach()
    retryWrapper = spy(new RequestRetryWrapper)
    assert(retryWrapper.numRetries == DefaultMaxNumRetries)
    dummyMethodInvoker = spy(new DummyMethodInvoker)
  }

  class DummyMethodInvoker {
    @throws[Exception]
    def someMethod() : String = {
      SomeValue
    }

    def createCheckedSupplierForSomeMethod() : dev.failsafe.function.CheckedSupplier[String] = {
      new dev.failsafe.function.CheckedSupplier[String]() {
        def get(): String = someMethod()
      }
    }
  }

  describe("wrapWithRetry") {
    // In case all retries fail, the last result or exception are returned or thrown
    // https://failsafe.dev/faqs/#how-to-i-throw-an-exception-when-retries-are-exceeded
    it("should throw the a FailSafeException exception in case all retries failed and last exception is checked") {
      // prepare
      doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doThrow(classOf[ApiException])
        .when(dummyMethodInvoker).someMethod()

      assertThrows[FailsafeException] {
        retryWrapper.wrapWithRetry(dummyMethodInvoker.createCheckedSupplierForSomeMethod())
      }

      // assert
      verify(dummyMethodInvoker, times(DefaultMaxNumAttempts)).someMethod()
    }

    it("should throw the last unchecked exception in case all retries failed") {
      // prepare
      doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doThrow(classOf[NullPointerException])
        .when(dummyMethodInvoker).someMethod()

      assertThrows[NullPointerException] {
        retryWrapper.wrapWithRetry(dummyMethodInvoker.createCheckedSupplierForSomeMethod())
      }

      // assert
      verify(dummyMethodInvoker, times(DefaultMaxNumAttempts)).someMethod()
    }

    it("should retry on any type of exception") {
        // prepare
        doThrow(classOf[RuntimeException])
          .doThrow(classOf[SocketException])
          .doThrow(classOf[ApiException])
          .doThrow(classOf[NullPointerException])
          .doCallRealMethod()
          .when(dummyMethodInvoker).someMethod()

        // test
        retryWrapper.wrapWithRetry(dummyMethodInvoker.createCheckedSupplierForSomeMethod())

        // assert
        verify(dummyMethodInvoker, times(5)).someMethod()
    }

    it("should retry until success within the boundaries of the allowed number of retries") {
      // prepare
      doThrow(classOf[Exception])
        .doThrow(classOf[Exception])
        .doCallRealMethod()
        .when(dummyMethodInvoker).someMethod()

      // test
      retryWrapper.wrapWithRetry(dummyMethodInvoker.createCheckedSupplierForSomeMethod())

      // assert
      verify(dummyMethodInvoker, times(3)).someMethod()
    }

    it("should use custom max number of retries") {
      retryWrapper = new RequestRetryWrapper(3)
      assert(retryWrapper.numRetries == 3)
    }
  }
}
