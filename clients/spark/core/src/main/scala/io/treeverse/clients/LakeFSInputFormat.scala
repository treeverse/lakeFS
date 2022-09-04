package io.treeverse.clients

import io.treeverse.clients.LakeFSContext._
import io.treeverse.lakefs.catalog.Entry
import io.treeverse.lakefs.graveler.committed.RangeData
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.SplitLocationInfo
import org.apache.hadoop.mapreduce._
import scalapb.GeneratedMessage
import scalapb.GeneratedMessageCompanion

import java.io.DataInput
import java.io.DataOutput
import java.io.File
import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import com.google.protobuf.ByteString

class GravelerSplit(var range: RangeData, var path: Path, var rangeID: Array[Byte])
    extends InputSplit
    with Writable {
  def this() = this(null, null, null)

  override def write(out: DataOutput): Unit = {
    out.writeInt(rangeID.length)
    out.write(rangeID)
    if (range != null) {
      val encodedRange = range.toByteArray
      out.writeInt(encodedRange.length)
      out.write(encodedRange)
    } else {
      out.writeInt(0)
    }
    val p = path.toString
    out.writeInt(p.length)
    out.writeChars(p)
  }

  override def readFields(in: DataInput): Unit = {
    val rangeIDLength = in.readInt()
    rangeID = new Array[Byte](rangeIDLength)
    in.readFully(rangeID)
    val encodedRangeLength = in.readInt()
    if (encodedRangeLength == 0) {
      range = null
    } else {
      val encodedRange = new Array[Byte](encodedRangeLength)
      in.readFully(encodedRange)
      range = RangeData.parseFrom(encodedRange)
    }
    val pathLength = in.readInt()
    val p = new StringBuilder
    for (_ <- 1 to pathLength) {
      p += in.readChar()
    }
    path = new Path(p.result)
  }
  override def getLength: Long = if (range == null) 0 else range.count

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
  var gravelerSplit: GravelerSplit = _
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val localFile = File.createTempFile("lakefs.", ".range")
    localFile.deleteOnExit()
    gravelerSplit = split.asInstanceOf[GravelerSplit]

    val fs = gravelerSplit.path.getFileSystem(context.getConfiguration)
    fs.copyToLocalFile(gravelerSplit.path, new Path(localFile.getAbsolutePath))
    // TODO(johnnyaug) should we cache this?
    val sstableReader =
      new SSTableReader(localFile.getAbsolutePath, companion)
    if (gravelerSplit.range == null) {
      val props = sstableReader.getProperties()
      if (new String(props.get("type").get) != "ranges") {
        return
      }
      gravelerSplit.range = new RangeData(
        ByteString.copyFrom(props.get("min_key").get),
        ByteString.copyFrom(props.get("max_key").get),
        new String(props.get("estimated_size_bytes").get).toLong,
        new String(props.get("count").get).toLong
      )
    }
    it = sstableReader.newIterator()
  }

  override def nextKeyValue: Boolean = {
    if (gravelerSplit.range == null) {
      return false
    }
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
  def read[Proto <: GeneratedMessage with scalapb.Message[Proto]](
      reader: SSTableReader[Proto]
  ): Seq[Item[Proto]] = reader.newIterator().toSeq
}

abstract class LakeFSBaseInputFormat extends InputFormat[Array[Byte], WithIdentifier[Entry]] {
  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext
  ): RecordReader[Array[Byte], WithIdentifier[Entry]] = {
    new EntryRecordReader(Entry.messageCompanion)
  }
}
class LakeFSCommitInputFormat extends LakeFSBaseInputFormat {
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
        new Path(apiClient.getRangeURL(repoName, new String(r.id))),
        r.id
      )
        // Scala / JRE not strong enough to handle List<FileSplit> as List<InputSplit>;
        // explicitly upcast to generate Seq[InputSplit].
        .asInstanceOf[InputSplit]
    )
  }.asJava
}

class LakeFSRepositoryInputFormat extends LakeFSBaseInputFormat {
  override def getSplits(job: JobContext): java.util.List[InputSplit] = {
    val conf = job.getConfiguration
    val repoName = conf.get(LAKEFS_CONF_JOB_REPO_NAME_KEY)
    var storageNamespace = conf.get(LAKEFS_CONF_JOB_STORAGE_NAMESPACE_KEY)
    if (StringUtils.isBlank(repoName) && StringUtils.isBlank(storageNamespace)) {
      throw new IllegalArgumentException(
        "Must specify either LAKEFS_CONF_JOB_REPO_NAME_KEY or LAKEFS_CONF_JOB_STORAGE_NAMESPACE_KEY"
      )
    }
    val apiClient = ApiClient.get(
      APIConfigurations(
        conf.get(LAKEFS_CONF_API_URL_KEY),
        conf.get(LAKEFS_CONF_API_ACCESS_KEY_KEY),
        conf.get(LAKEFS_CONF_API_SECRET_KEY_KEY),
        conf.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY),
        conf.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY)
      )
    )
    if (StringUtils.isBlank(storageNamespace)) {
      storageNamespace = apiClient.getStorageNamespace(repoName, StorageClientType.HadoopFS)
    }
    var nsURI = URI.create(storageNamespace)
    nsURI = new URI(nsURI.getScheme,
                    nsURI.getUserInfo,
                    nsURI.getHost,
                    nsURI.getPort,
                    nsURI.getPath + (if (nsURI.getPath.endsWith("/")) "_lakefs" else "/_lakefs"),
                    null,
                    null
                   )
    val fs = FileSystem.get(nsURI, conf)
    val it = fs.listFiles(new Path(nsURI.toString), false)
    val splits = new ListBuffer[InputSplit]()
    while (it.hasNext) {
      val file = it.next()
      splits += new GravelerSplit(
        null,
        file.getPath,
        file.getPath.getName.getBytes
      )
    }
    return splits.asJava
  }
}
