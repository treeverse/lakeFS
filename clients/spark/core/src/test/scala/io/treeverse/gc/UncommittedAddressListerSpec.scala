package io.treeverse.gc

import io.lakefs.clients.api.model.{PrepareGCUncommittedResponse}
import io.treeverse.clients.{ApiClient, SparkSessionSetup}
import org.scalatest.funspec._
import org.scalatest.matchers._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito

class UncommittedAddressListerSpec
    extends AnyFunSpec
    with SparkSessionSetup
    with should.Matchers
    with MockitoSugar {
  describe(".listData") {
    val mockClient = mock[ApiClient]
    val repo = "example-repo"
    val runID = "some-run-id"
    val uncommittedLocation = "some-uncommitted-location"
    it("should get the run ID - single file") {
      val resp = new PrepareGCUncommittedResponse()

      resp.runId(runID)
      resp.gcUncommittedLocation(uncommittedLocation)
      resp.setContinuationToken("")
      Mockito
        .when(mockClient.prepareGarbageCollectionUncommitted(repo, null))
        .thenReturn(resp)

      withSparkSession(spark => {
        val info = new APIUncommittedAddressLister(mockClient)
          .listUncommittedAddresses(spark, repo)
        info.runID should be(runID)
        info.uncommitedLocation should be(uncommittedLocation)
      })
    }

    it("should get the run ID - two files") {
      val respFirst = new PrepareGCUncommittedResponse()
      val token = "token"

      respFirst.runId(runID)
      respFirst.gcUncommittedLocation(uncommittedLocation)
      respFirst.setContinuationToken("token")
      Mockito
        .when(mockClient.prepareGarbageCollectionUncommitted(repo, token))
        .thenReturn(respFirst)
      val respSecond = respFirst
      respSecond.setContinuationToken("")
      Mockito
        .when(mockClient.prepareGarbageCollectionUncommitted(repo, null))
        .thenReturn(respFirst)

      withSparkSession(spark => {
        val info = new APIUncommittedAddressLister(mockClient)
          .listUncommittedAddresses(spark, repo)
        info.runID should be(runID)
        info.uncommitedLocation should be(uncommittedLocation)
      })
    }

  }
}
