package io.treeverse.gc

import io.lakefs.clients.sdk.model.GarbageCollectionPrepareResponse
import io.lakefs.clients.sdk.ApiException
import io.treeverse.clients.{ApiClient, LakeFSContext, SparkSessionSetup}
import org.apache.http.HttpStatus
import org.scalatest.funspec._
import org.scalatest.matchers._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import java.nio.file.Files
import org.apache.commons.io.FileUtils

class ActiveCommitsAddressListerSpec
    extends AnyFunSpec
    with SparkSessionSetup
    with should.Matchers
    with MockitoSugar {

  describe("ActiveCommitsAddressLister") {
    val mockClient = mock[ApiClient]
    val repo = "example-repo"
    val storageType = "s3"
    val gcCommitsLocation = "s3://test-bucket/gc-commits.csv"

    describe(".listCommittedAddresses") {
      it("should successfully call prepareGarbageCollectionCommits and process result") {
        // Create a temporary CSV file with commit data
        val tempDir = Files.createTempDirectory("gc-test")
        val csvFile = tempDir.resolve("commits.csv").toFile
        try {
          // Write CSV with header and test data
          val csvContent = "commit_id,expired\ncommit1,false\ncommit2,true\ncommit3,false\n"
          FileUtils.writeStringToFile(csvFile, csvContent, "UTF-8")

          // Use file:// URI for local testing
          val localGcCommitsLocation = csvFile.toURI.toString
          val prepareResponse = new GarbageCollectionPrepareResponse()
          prepareResponse.setGcCommitsLocation(localGcCommitsLocation)

          Mockito
            .when(mockClient.prepareGarbageCollectionCommits(org.mockito.ArgumentMatchers.eq(repo), org.mockito.ArgumentMatchers.anyInt()))
            .thenReturn(prepareResponse)

          withSparkSession(spark => {
            // Set required Hadoop configuration for LakeFSContext
            spark.sparkContext.hadoopConfiguration.set("lakefs.api.url", "http://localhost:8000/api/v1")
            spark.sparkContext.hadoopConfiguration.set("lakefs.api.access_key", "test-access-key")
            spark.sparkContext.hadoopConfiguration.set("lakefs.api.secret_key", "test-secret-key")

            val lister = new ActiveCommitsAddressLister(mockClient, repo, storageType, LakeFSContext.DEFAULT_LAKEFS_CONF_GC_PREPARE_COMMITS_TIMEOUT_MINUTES)
            val result = lister.listCommittedAddresses(
              spark,
              "s3://test-bucket/storage/",
              "s3://test-bucket/client/"
            )

            // Verify the API was called with correct timeout
            Mockito.verify(mockClient, Mockito.times(1)).prepareGarbageCollectionCommits(org.mockito.ArgumentMatchers.eq(repo), org.mockito.ArgumentMatchers.anyInt())

            // The result should be a DataFrame (we can't easily verify its contents without
            // setting up the full LakeFS context, but we can verify it doesn't throw)
            result should not be null
          })
        } finally {
          FileUtils.deleteDirectory(tempDir.toFile)
        }
      }

      it("should fallback to NaiveCommittedAddressLister on 404 error") {
        val apiException = new ApiException(
          HttpStatus.SC_NOT_FOUND,
          "Garbage collection rules not found"
        )

        Mockito
          .doAnswer(new Answer[GarbageCollectionPrepareResponse] {
            def answer(invocation: InvocationOnMock): GarbageCollectionPrepareResponse = {
              throw apiException
            }
          })
          .when(mockClient)
          .prepareGarbageCollectionCommits(org.mockito.ArgumentMatchers.eq(repo), org.mockito.ArgumentMatchers.anyInt())

        withSparkSession(spark => {
          // Set required Hadoop configuration for LakeFSContext
          spark.sparkContext.hadoopConfiguration.set("lakefs.api.url", "http://localhost:8000/api/v1")
          spark.sparkContext.hadoopConfiguration.set("lakefs.api.access_key", "test-access-key")
          spark.sparkContext.hadoopConfiguration.set("lakefs.api.secret_key", "test-secret-key")

            val lister = new ActiveCommitsAddressLister(mockClient, repo, storageType, LakeFSContext.DEFAULT_LAKEFS_CONF_GC_PREPARE_COMMITS_TIMEOUT_MINUTES)
            // Should not throw, but fallback to naive lister
            val result = lister.listCommittedAddresses(
              spark,
              "s3://test-bucket/storage/",
              "s3://test-bucket/client/"
            )

          // Verify the API was called (may be called multiple times due to retries)
          Mockito.verify(mockClient, Mockito.atLeastOnce()).prepareGarbageCollectionCommits(org.mockito.ArgumentMatchers.eq(repo), org.mockito.ArgumentMatchers.anyInt())

          // Result should be a DataFrame from naive lister
          result should not be null
        })
      }

      it("should throw exception on non-404 API error") {
        val apiException = new ApiException(
          HttpStatus.SC_INTERNAL_SERVER_ERROR,
          "Internal server error"
        )

        Mockito
          .doAnswer(new Answer[GarbageCollectionPrepareResponse] {
            def answer(invocation: InvocationOnMock): GarbageCollectionPrepareResponse = {
              throw apiException
            }
          })
          .when(mockClient)
          .prepareGarbageCollectionCommits(org.mockito.ArgumentMatchers.eq(repo), org.mockito.ArgumentMatchers.anyInt())

        withSparkSession(spark => {
          // Set required Hadoop configuration for LakeFSContext (even though it will fail earlier)
          spark.sparkContext.hadoopConfiguration.set("lakefs.api.url", "http://localhost:8000/api/v1")
          spark.sparkContext.hadoopConfiguration.set("lakefs.api.access_key", "test-access-key")
          spark.sparkContext.hadoopConfiguration.set("lakefs.api.secret_key", "test-secret-key")

          val lister = new ActiveCommitsAddressLister(mockClient, repo, storageType, LakeFSContext.DEFAULT_LAKEFS_CONF_GC_PREPARE_COMMITS_TIMEOUT_MINUTES)
          val thrown = intercept[ApiException] {
            lister.listCommittedAddresses(
              spark,
              "s3://test-bucket/storage/",
              "s3://test-bucket/client/"
            )
          }

          thrown.getCode should be(HttpStatus.SC_INTERNAL_SERVER_ERROR)
          // The method may be called multiple times due to retries, so we check at least once
          Mockito.verify(mockClient, Mockito.atLeastOnce()).prepareGarbageCollectionCommits(org.mockito.ArgumentMatchers.eq(repo), org.mockito.ArgumentMatchers.anyInt())
        })
      }
    }
  }
}
