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
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.RemoteIterator

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

class LakeFSCommitInputFormatSpec
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
      splits.asScala.toList.map(_.asInstanceOf[GravelerSplit].rangeID).toSet should be(
        Set("r1", "r3")
      )
    }

    it("should return ranges combined from 2 commits") {
      Mockito
        .when(conf.getStrings(LakeFSContext.LAKEFS_CONF_JOB_COMMIT_IDS_KEY))
        .thenReturn(Array("c1", "c2"))
      val splits = inputFormat.getSplits(context)
      splits.asScala.toList.map(_.asInstanceOf[GravelerSplit].rangeID).toSet should be(
        Set("r1", "r2", "r3")
      )
    }

    it("should return ranges combined from 3 commits") {
      Mockito
        .when(conf.getStrings(LakeFSContext.LAKEFS_CONF_JOB_COMMIT_IDS_KEY))
        .thenReturn(Array("c1", "c2", "c3"))
      val splits = inputFormat.getSplits(context)
      splits.asScala.toList.map(_.asInstanceOf[GravelerSplit].rangeID).toSet should be(
        Set("r1", "r2", "r3", "r4", "r5")
      )
    }
  }
}
private class MockRemoteIterator[FileStatus](iterator: Iterator[FileStatus])
    extends RemoteIterator[FileStatus] {

  override def hasNext(): Boolean = iterator.hasNext

  override def next(): FileStatus = iterator.next
}

class LakeFSAllRangesInputFormatSpec
    extends AnyFunSpec
    with Matchers
    with MockitoSugar
    with OneInstancePerTest {
  val repoName = "repo1"

  val inputFormat = new LakeFSAllRangesInputFormat()
  val mockFileSystem = mock[FileSystem]
  val mockClient = mock[ApiClient]
  inputFormat.apiClientGetter = (APIConfigurations) => mockClient
  inputFormat.fileSystemGetter = (URI, Configuration) => mockFileSystem
  val context = mock[JobContext]
  val conf = mock[Configuration]
  Mockito.when(conf.get(LakeFSContext.LAKEFS_CONF_JOB_REPO_NAME_KEY)).thenReturn(repoName)
  Mockito.when(context.getConfiguration()).thenReturn(conf)
  Mockito
    .when(mockClient.getStorageNamespace(repoName, StorageClientType.HadoopFS))
    .thenReturn("s3a://bucket/")
  Mockito
    .when(mockFileSystem.exists(mockito.Matchers.eq(new Path("s3a://bucket/_lakefs"))))
    .thenReturn(true)
  it("should return ranges files as splits") {
    Mockito
      .when(
        mockFileSystem.listFiles(mockito.Matchers.eq(new Path("s3a://bucket/_lakefs")),
                                 mockito.Matchers.eq(false)
                                )
      )
      .thenReturn(
        new MockRemoteIterator(
          Array("s3a://bucket/_lakefs/dummy",
                "s3a://bucket/_lakefs/r1",
                "s3a://bucket/_lakefs/r2",
                "s3a://bucket/_lakefs/r3"
               )
            .map(n => {
              val stat = new FileStatus()
              stat.setPath(new Path(n))
              new LocatedFileStatus(stat, Array[BlockLocation]())
            })
            .toIterator
        )
      )
    val splits = inputFormat.getSplits(context)
    splits.asScala.toList.map(_.asInstanceOf[GravelerSplit].rangeID).toSet should be(
      Set("r1", "r2", "r3")
    )
  }
}
