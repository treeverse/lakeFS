package io.treeverse.clients

import io.treeverse.lakefs.catalog.Entry
import io.treeverse.clients.LakeFSContext._
import io.treeverse.lakefs.graveler.committed.RangeData
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.SplitLocationInfo
import org.apache.hadoop.mapreduce._
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.{DataInput, DataOutput, File}
import scala.collection.JavaConverters._

class GravelerSplit(var range: RangeData, var path: Path) extends InputSplit with Writable {
  def this() = this(null, null)

  override def write(out: DataOutput): Unit = {
    val encodedRange = range.toByteArray
    out.writeInt(encodedRange.length)
    out.write(encodedRange)
    val p = path.toString
    out.writeInt(p.length)
    out.writeChars(p)
  }

  override def readFields(in: DataInput): Unit = {
    val encodedRangeLength = in.readInt()
    val encodedRange = new Array[Byte](encodedRangeLength)
    in.readFully(encodedRange)
    range = RangeData.parseFrom(encodedRange)
    val pathLength = in.readInt()
    val p = new StringBuilder
    for (_ <- 1 to pathLength) {
      p += in.readChar()
    }
    path = new Path(p.result)
  }

  override def getLength: Long = range.estimatedSize

  override def getLocations: Array[String] = Array.empty[String]

  override def getLocationInfo: Array[SplitLocationInfo] =
    Array.empty[SplitLocationInfo]
}

class WithIdentifier[Proto <: GeneratedMessage with scalapb.Message[Proto]](
    val id: Array[Byte],
    val message: Proto
) {}

class EntryRecordReader[Proto <: GeneratedMessage with scalapb.Message[Proto]](
    companion: GeneratedMessageCompanion[Proto]
) extends RecordReader[Array[Byte], WithIdentifier[Proto]] {
  var it: SSTableIterator[Proto] = _
  var item: Item[Proto] = _

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val localFile = File.createTempFile("lakefs.", ".range")
    localFile.deleteOnExit()
    val gravelerSplit = split.asInstanceOf[GravelerSplit]
    val fs = gravelerSplit.path.getFileSystem(context.getConfiguration)
    fs.copyToLocalFile(gravelerSplit.path, new Path(localFile.getAbsolutePath))
    // TODO(johnnyaug) should we cache this?
    val sstableReader =
      new SSTableReader(localFile.getAbsolutePath, companion)
    it = sstableReader.newIterator()
  }

  override def nextKeyValue: Boolean = {
    if (!it.hasNext) {
      return false
    }
    item = it.next()
    true
  }

  override def getCurrentKey: Array[Byte] = item.key

  override def getCurrentValue = new WithIdentifier(item.id, item.message)

  override def close() = Unit

  override def getProgress: Float = {
    0 // TODO(johnnyaug) complete
  }
}

object LakeFSInputFormat {
  private def read[Proto <: GeneratedMessage with scalapb.Message[Proto]](
      reader: SSTableReader[Proto]
  ): Seq[Item[Proto]] = reader.newIterator().toSeq
}

class LakeFSInputFormat extends InputFormat[Array[Byte], WithIdentifier[Entry]] {
  import LakeFSInputFormat._

  override def getSplits(job: JobContext): java.util.List[InputSplit] = {
    val conf = job.getConfiguration
    val repoName = conf.get(LAKEFS_CONF_JOB_REPO_NAME_KEY)
    val commitID = conf.get(LAKEFS_CONF_JOB_COMMIT_ID_KEY)
    val apiClient = ApiClient.get(
      APIConfigurations(
        conf.get(LAKEFS_CONF_API_URL_KEY),
        conf.get(LAKEFS_CONF_API_ACCESS_KEY_KEY),
        conf.get(LAKEFS_CONF_API_SECRET_KEY_KEY),
        conf.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY),
        conf.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
      )
    )
    val metaRangeURL = apiClient.getMetaRangeURL(repoName, commitID)
    val rangesReader: SSTableReader[RangeData] =
      SSTableReader.forMetaRange(job.getConfiguration, metaRangeURL)
    val ranges = read(rangesReader)
    ranges.map(r =>
      new GravelerSplit(
        r.message,
        new Path(apiClient.getRangeURL(repoName, new String(r.id)))
      )
        // Scala / JRE not strong enough to handle List<FileSplit> as List<InputSplit>;
        // explicitly upcast to generate Seq[InputSplit].
        .asInstanceOf[InputSplit]
    )
  }.asJava

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext
  ): RecordReader[Array[Byte], WithIdentifier[Entry]] = {
    new EntryRecordReader(Entry.messageCompanion)
  }
}
