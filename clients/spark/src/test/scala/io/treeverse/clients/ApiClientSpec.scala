package io.treeverse.clients

import io.lakefs.clients.sdk.ApiException
import io.lakefs.clients.sdk.model.{
  GarbageCollectionPrepareResponse,
  PrepareGarbageCollectionCommitsAsyncCreation,
  PrepareGarbageCollectionCommitsStatus
}
import org.apache.http.HttpStatus
import org.scalatest.funspec._
import org.scalatest.matchers.should._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito
import org.mockito.ArgumentMatchers.anyString

import java.net.URI

class ApiClientSpec extends AnyFunSpec with Matchers with MockitoSugar {
  describe("translateURI") {
    describe("s3") {
      val translate =
        (u: String) => ApiClient.translateURI(new URI(u), StorageUtils.StorageTypeS3).toString
      it("should translate to s3a") {
        translate("s3://bucket/path/to/object") should be("s3a://bucket/path/to/object")
      }

      it("should translate unadorned bucket to a bucket with no slashes") {
        translate("s3://bucket") should be("s3a://bucket")
      }

      it("should translate unadorned bucket with a slash to a bucket with a slash") {
        translate("s3://bucket/") should be("s3a://bucket/")
      }

      it(
        "should translate bucket with path ending in a slash to a bucket with a path ending in a slash"
      ) {
        translate("s3://bucket/path/") should be("s3a://bucket/path/")
      }

      it("should preserve multiple slashes") {
        translate("s3://two//three///") should be("s3a://two//three///")
      }
    }

    describe("Azure") {
      val translate =
        (u: String) => ApiClient.translateURI(new URI(u), StorageUtils.StorageTypeAzure).toString
      it("should translate any protocol to abfs") {
        translate("https://account.example.net/container/path/to/blob") should be(
          "abfs://container@account.dfs.core.windows.net/path/to/blob"
        )
        translate("ex://account.example.net/container/path/to/blob") should be(
          "abfs://container@account.dfs.core.windows.net/path/to/blob"
        )
      }

      it("should handle empty paths") {
        // TODO(lynn): Trailing slashes might well be incorrect here.
        translate("https://account.example.net/container/") should be(
          "abfs://container@account.dfs.core.windows.net/"
        )
        translate("https://account.example.net/container") should be(
          "abfs://container@account.dfs.core.windows.net/"
        )
      }
    }
    describe("GCS") {
      val pathUri = "gs://bucket/path/to/blob"
      val translate =
        (u: String) => ApiClient.translateURI(new URI(u), StorageUtils.StorageTypeGCS).toString
      it("should return the same URI") {
        translate(pathUri) should be(pathUri)
      }
    }
  }

  describe("prepareGarbageCollectionCommits") {
    val repoName = "test-repo"
    val apiUrl = "http://localhost:8000/api/v1"
    val accessKey = "test-access-key"
    val secretKey = "test-secret-key"

    it("should fallback to sync API when async API returns 500") {
      val conf = APIConfigurations(apiUrl, accessKey, secretKey)
      val apiClient = ApiClient.get(conf)

      // Create a mock of InternalApi
      val mockInternalApi = Mockito.mock(classOf[io.lakefs.clients.sdk.InternalApi])

      // Create mock request builder for async API
      val asyncApiBuilder = Mockito.mock(classOf[io.lakefs.clients.sdk.InternalApi#APIprepareGarbageCollectionCommitsAsyncRequest])
      Mockito.when(asyncApiBuilder.execute()).thenThrow(new ApiException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Internal server error"))
      Mockito.when(mockInternalApi.prepareGarbageCollectionCommitsAsync(anyString())).thenReturn(asyncApiBuilder)

      // Create mock request builder for sync API
      val syncResponse = new GarbageCollectionPrepareResponse()
      syncResponse.setGcCommitsLocation("s3://test-bucket/gc-commits.csv")
      val syncApiBuilder = Mockito.mock(classOf[io.lakefs.clients.sdk.InternalApi#APIprepareGarbageCollectionCommitsRequest])
      Mockito.when(syncApiBuilder.execute()).thenReturn(syncResponse)
      Mockito.when(mockInternalApi.prepareGarbageCollectionCommits(anyString())).thenReturn(syncApiBuilder)

      // Replace the internalApi with our mock using the test method
      apiClient.setInternalApiForTesting(mockInternalApi)

      // Call the method - it should fallback to sync API
      val result = apiClient.prepareGarbageCollectionCommits(repoName, timeoutMinutes = 1)

      // Verify async API was called first (may be retried by retry wrapper)
      Mockito.verify(mockInternalApi, Mockito.atLeastOnce()).prepareGarbageCollectionCommitsAsync(repoName)

      // Verify sync API was called as fallback
      Mockito.verify(mockInternalApi, Mockito.times(1)).prepareGarbageCollectionCommits(repoName)

      // Verify result
      result should not be null
      result.getGcCommitsLocation should be("s3://test-bucket/gc-commits.csv")
    }

    it("should successfully complete async API call without fallback") {
      val conf = APIConfigurations(apiUrl, accessKey, secretKey)
      val apiClient = ApiClient.get(conf)

      // Create a mock of InternalApi
      val mockInternalApi = Mockito.mock(classOf[io.lakefs.clients.sdk.InternalApi])

      val taskId = "test-task-id"

      // Mock async API creation
      val asyncCreation = new PrepareGarbageCollectionCommitsAsyncCreation()
      asyncCreation.setId(taskId)

      val asyncApiBuilder = Mockito.mock(classOf[io.lakefs.clients.sdk.InternalApi#APIprepareGarbageCollectionCommitsAsyncRequest])
      Mockito.when(asyncApiBuilder.execute()).thenReturn(asyncCreation)
      Mockito.when(mockInternalApi.prepareGarbageCollectionCommitsAsync(anyString())).thenReturn(asyncApiBuilder)

      // Mock status API to return completed status immediately
      val status = new PrepareGarbageCollectionCommitsStatus()
      status.setCompleted(true)
      val result = new GarbageCollectionPrepareResponse()
      result.setGcCommitsLocation("s3://test-bucket/gc-commits.csv")
      status.setResult(result)

      val statusApiBuilder = Mockito.mock(classOf[io.lakefs.clients.sdk.InternalApi#APIprepareGarbageCollectionCommitsStatusRequest])
      Mockito.when(statusApiBuilder.execute()).thenReturn(status)
      Mockito.when(mockInternalApi.prepareGarbageCollectionCommitsStatus(anyString(), anyString())).thenReturn(statusApiBuilder)

      // Replace the internalApi with our mock using the test method
      apiClient.setInternalApiForTesting(mockInternalApi)

      // Call the method
      val response = apiClient.prepareGarbageCollectionCommits(repoName, timeoutMinutes = 1)

      // Verify async API was called
      Mockito.verify(mockInternalApi, Mockito.times(1)).prepareGarbageCollectionCommitsAsync(repoName)

      // Verify status API was called
      Mockito.verify(mockInternalApi, Mockito.atLeastOnce()).prepareGarbageCollectionCommitsStatus(repoName, taskId)

      // Verify sync API was NOT called (no fallback)
      Mockito.verify(mockInternalApi, Mockito.never()).prepareGarbageCollectionCommits(anyString())

      // Verify result
      response should not be null
      response.getGcCommitsLocation should be("s3://test-bucket/gc-commits.csv")
    }

    it("should throw exception on non-500 API error without fallback") {
      val conf = APIConfigurations(apiUrl, accessKey, secretKey)
      val apiClient = ApiClient.get(conf)

      // Create a mock of InternalApi
      val mockInternalApi = Mockito.mock(classOf[io.lakefs.clients.sdk.InternalApi])

      // Create mock request builder for async API that throws 404 on execute()
      val asyncApiBuilder = Mockito.mock(classOf[io.lakefs.clients.sdk.InternalApi#APIprepareGarbageCollectionCommitsAsyncRequest])
      Mockito.when(asyncApiBuilder.execute()).thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "Not found"))
      Mockito.when(mockInternalApi.prepareGarbageCollectionCommitsAsync(anyString())).thenReturn(asyncApiBuilder)

      // Replace the internalApi with our mock using the test method
      apiClient.setInternalApiForTesting(mockInternalApi)

      // Call the method - it should throw the exception without fallback
      val thrown = intercept[ApiException] {
        apiClient.prepareGarbageCollectionCommits(repoName, timeoutMinutes = 1)
      }

      thrown.getCode should be(HttpStatus.SC_NOT_FOUND)

      // Verify async API was called (may be retried by retry wrapper)
      Mockito.verify(mockInternalApi, Mockito.atLeastOnce()).prepareGarbageCollectionCommitsAsync(repoName)

      // Verify sync API was NOT called (no fallback for non-500 errors)
      Mockito.verify(mockInternalApi, Mockito.never()).prepareGarbageCollectionCommits(anyString())
    }
  }
}
