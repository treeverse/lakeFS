package io.treeverse.clients

import com.google.protobuf.ByteString
import io.treeverse.lakefs.graveler.committed.RangeData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.JobContext
import org.mockito
import org.mockito.Mockito
import org.scalatest.funspec._
import org.scalatest.matchers.should._
import org.scalatestplus.mockito.MockitoSugar
import scala.collection.JavaConverters._

import scala.collection.mutable
import org.scalatest.OneInstancePerTest

object LakeFSInputFormatSpec {
  def getItem(rangeID: String): Item[RangeData] = new Item(
    rangeID.getBytes(),
    rangeID.getBytes(),
    new RangeData(
      ByteString.copyFrom(rangeID + "-k0min", "UTF8"),
      ByteString.copyFrom(rangeID + "-k1max", "UTF8")
    )
  )
}

class LakeFSInputFormatSpec
    extends AnyFunSpec
    with Matchers
    with MockitoSugar
    with OneInstancePerTest {
  val commitToRanges =
    Map("c1" -> Array("r1", "r2"), "c2" -> Array("r1", "r3"), "c3" -> Array("r2", "r3", "r4", "r5"))

  describe("getSplits") {
    val mockClient = mock[ApiClient]
    val context = mock[JobContext]
    val conf = mock[Configuration]
    val repoName = "repo1"
    Mockito.when(conf.get(LakeFSContext.LAKEFS_CONF_JOB_REPO_NAME_KEY)).thenReturn(repoName)
    Mockito.when(context.getConfiguration()).thenReturn(conf)

    val readers = mutable.Map[String, SSTableReader[RangeData]]()
    commitToRanges.foreach(e => {
      val metarangeReader = mock[SSTableReader[RangeData]]
      val sstableIterator = mock[SSTableIterator[RangeData]]
      Mockito
        .when(metarangeReader.newIterator())
        .thenReturn(e._2.map(LakeFSInputFormatSpec.getItem).toSeq.iterator)
      readers(e._1) = metarangeReader
    })
    Mockito
      .when(mockClient.getRangeURL(mockito.Matchers.eq(repoName), mockito.Matchers.anyString()))
      .thenAnswer(new mockito.stubbing.Answer[String] {
        override def answer(i: mockito.invocation.InvocationOnMock): String =
          "s3a://bucket/" + i.getArguments()(1).asInstanceOf[String]

      })
    Mockito
      .when(mockClient.getMetaRangeURL(mockito.Matchers.eq(repoName), mockito.Matchers.anyString))
      .thenAnswer(new mockito.stubbing.Answer[String] {
        override def answer(i: mockito.invocation.InvocationOnMock): String =
          i.getArguments()(1).asInstanceOf[String].replace("c", "mr")
      })
    val inputFormat = new LakeFSCommitInputFormat()
    inputFormat.apiClientGetter = (APIConfigurations) => mockClient
    inputFormat.metarangeReaderGetter = (Configuration, metaRangeURL: String, Boolean) => {
      readers(metaRangeURL.replace("mr", "c"))
    }

    it("should return ranges combined from a single commit") {
      Mockito
        .when(conf.getStrings(LakeFSContext.LAKEFS_CONF_JOB_COMMIT_IDS_KEY))
        .thenReturn(Array("c2"))
      val splits = inputFormat.getSplits(context)
      splits.toList.map(_.asInstanceOf[GravelerSplit].rangeID).toSet should be(Set("r1", "r3"))
    }

    it("should return ranges combined from 2 commits") {
      Mockito
        .when(conf.getStrings(LakeFSContext.LAKEFS_CONF_JOB_COMMIT_IDS_KEY))
        .thenReturn(Array("c1", "c2"))
      val splits = inputFormat.getSplits(context)
      splits.toList.map(_.asInstanceOf[GravelerSplit].rangeID).toSet should be(
        Set("r1", "r2", "r3")
      )
    }

    it("should return ranges combined from 3 commits") {
      Mockito
        .when(conf.getStrings(LakeFSContext.LAKEFS_CONF_JOB_COMMIT_IDS_KEY))
        .thenReturn(Array("c1", "c2", "c3"))
      val splits = inputFormat.getSplits(context)
      splits.toList.map(_.asInstanceOf[GravelerSplit].rangeID).toSet should be(
        Set("r1", "r2", "r3", "r4", "r5")
      )
    }
  }
}
