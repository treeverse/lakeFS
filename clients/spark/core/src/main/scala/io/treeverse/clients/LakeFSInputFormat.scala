package io.treeverse.clients

import io.treeverse.clients.LakeFSContext._
import io.treeverse.lakefs.catalog.Entry
import io.treeverse.lakefs.graveler.committed.RangeData
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.SplitLocationInfo
import org.apache.hadoop.mapreduce._
import org.apache.spark.TaskContext
import org.slf4j.{Logger, LoggerFactory}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.{DataInput, DataOutput, File}
import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object GravelerSplit {
  val logger: Logger = LoggerFactory.getLogger(getClass.toString)
}
class GravelerSplit(
    var path: Path,
    var rangeID: String,
    var byteSize: Long,
    var isValidated: Boolean
) extends InputSplit
    with Writable {
  def this() = this(null, null, 0, false)

  import GravelerSplit._
  override def write(out: DataOutput): Unit = {
    out.writeLong(byteSize)
    out.writeInt(rangeID.length)
    out.writeChars(rangeID)
    val p = path.toString
    out.writeInt(p.length)
    out.writeChars(p)
    out.writeBoolean(isValidated)
  }

  override def readFields(in: DataInput): Unit = {
    byteSize = in.readLong()
    val rangeIDLength = in.readInt()
    val rangeIDBuilder = new StringBuilder
    for (_ <- 1 to rangeIDLength) {
      rangeIDBuilder += in.readChar()
    }
    rangeID = rangeIDBuilder.toString
    val pathLength = in.readInt()
    val p = new StringBuilder
    for (_ <- 1 to pathLength) {
      p += in.readChar()
    }
    path = new Path(p.result)
    isValidated = in.readBoolean()
    logger.debug(s"Read split $this")
  }
  override def getLength: Long = byteSize

  override def getLocations: Array[String] = Array.empty[String]

  override def getLocationInfo: Array[SplitLocationInfo] =
    Array.empty[SplitLocationInfo]

  override def toString: String =
    s"GravelerSplit(path=$path, rangeID=$rangeID, byteSize=$byteSize, isValidated=$isValidated)"
}

class WithIdentifier[Proto <: GeneratedMessage with scalapb.Message[Proto]](
    val id: Array[Byte],
    val message: Proto,
    val rangeID: String
) {}

class EntryRecordReader[Proto <: GeneratedMessage with scalapb.Message[Proto]](
    companion: GeneratedMessageCompanion[Proto]
) extends RecordReader[Array[Byte], WithIdentifier[Proto]] {
  val logger: Logger = LoggerFactory.getLogger(getClass.toString)

  private var sstableReader: SSTableReader[Proto] = _
  var localFile: java.io.File = _
  var it: SSTableIterator[Proto] = _
  var item: Item[Proto] = _
  var rangeID: String = ""
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    localFile = File.createTempFile("lakefs.", ".range")
    // Cleanup the local file - using the same technic as other data sources:
    // https://github.com/apache/spark/blob/c0b1735c0bfeb1ff645d146e262d7ccd036a590e/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/text/TextFileFormat.scala#L123
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => localFile.delete()))

    val gravelerSplit = split.asInstanceOf[GravelerSplit]

    val fs = gravelerSplit.path.getFileSystem(context.getConfiguration)
    fs.copyToLocalFile(false, gravelerSplit.path, new Path(localFile.getAbsolutePath), true)
    // TODO(johnnyaug) should we cache this?
    sstableReader = new SSTableReader(localFile.getAbsolutePath, companion, true)
    if (!gravelerSplit.isValidated) {
      // this file may not be a valid range file, validate it
      val props = sstableReader.getProperties
      logger.debug(s"Props: $props")
      if (new String(props("type")) != "ranges" || props.contains("entity")) {
        return
      }
    }
    rangeID = gravelerSplit.rangeID
    logger.debug(s"Initializing reader for split $gravelerSplit")
    it = sstableReader.newIterator()
  }

  override def nextKeyValue: Boolean = {
    if (rangeID == "") {
      return false
    }
    if (!it.hasNext) {
      return false
    }
    item = it.next()
    true
  }

  override def getCurrentKey: Array[Byte] = item.key

  override def getCurrentValue = new WithIdentifier(item.id, item.message, rangeID)

  override def close(): Unit = {
    localFile.delete()
    sstableReader.close()
  }

  override def getProgress: Float = {
    0 // TODO(johnnyaug) complete
  }
}

object LakeFSInputFormat {
  val logger: Logger = LoggerFactory.getLogger(getClass.toString)
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
private class Range(val id: String, val estimatedSize: Long)

class LakeFSCommitInputFormat extends LakeFSBaseInputFormat {
  import LakeFSInputFormat._
  override def getSplits(job: JobContext): java.util.List[InputSplit] = {
    val conf = job.getConfiguration
    val repoName = conf.get(LAKEFS_CONF_JOB_REPO_NAME_KEY)
    val commitIDs = conf.getStrings(LAKEFS_CONF_JOB_COMMIT_IDS_KEY)
    val apiClient = ApiClient.get(
      APIConfigurations(
        conf.get(LAKEFS_CONF_API_URL_KEY),
        conf.get(LAKEFS_CONF_API_ACCESS_KEY_KEY),
        conf.get(LAKEFS_CONF_API_SECRET_KEY_KEY),
        conf.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY),
        conf.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY),
        conf.get(LAKEFS_CONF_JOB_SOURCE_NAME_KEY, "input_format")
      )
    )
    val ranges = scala.collection.mutable.Set[Range]()
    commitIDs.foreach(commitID => {
      val metaRangeURL = apiClient.getMetaRangeURL(repoName, commitID)
      val rangesReader: SSTableReader[RangeData] =
        SSTableReader.forMetaRange(job.getConfiguration, metaRangeURL)
      ranges ++= (read(rangesReader).map(rd =>
        new Range(new String(rd.id), rd.message.estimatedSize)
      ))
    })
    val res = ranges.map(r =>
      new GravelerSplit(
        new Path(apiClient.getRangeURL(repoName, r.id)),
        new String(r.id),
        r.estimatedSize,
        true
      )
        // Scala / JRE not strong enough to handle List<FileSplit> as List<InputSplit>;
        // explicitly upcast to generate Seq[InputSplit].
        .asInstanceOf[InputSplit]
    )
    logger.debug(s"Returning ${res.size} splits")
    res
  }.toList.asJava
}

class LakeFSAllRangesInputFormat extends LakeFSBaseInputFormat {
  import LakeFSInputFormat._
  override def getSplits(job: JobContext): java.util.List[InputSplit] = {
    val conf = job.getConfiguration
    val repoName = conf.get(LAKEFS_CONF_JOB_REPO_NAME_KEY)
    var storageNamespace = conf.get(LAKEFS_CONF_JOB_STORAGE_NAMESPACE_KEY)
    if (
      (StringUtils.isBlank(repoName) && StringUtils.isBlank(storageNamespace)) ||
      (StringUtils.isNotBlank(repoName) && StringUtils.isNotBlank(storageNamespace))
    ) {
      throw new IllegalArgumentException(
        "Must specify exactly one of LAKEFS_CONF_JOB_REPO_NAME_KEY or LAKEFS_CONF_JOB_STORAGE_NAMESPACE_KEY"
      )
    }
    val apiClient = ApiClient.get(
      APIConfigurations(
        conf.get(LAKEFS_CONF_API_URL_KEY),
        conf.get(LAKEFS_CONF_API_ACCESS_KEY_KEY),
        conf.get(LAKEFS_CONF_API_SECRET_KEY_KEY),
        conf.get(LAKEFS_CONF_API_CONNECTION_TIMEOUT_SEC_KEY),
        conf.get(LAKEFS_CONF_API_READ_TIMEOUT_SEC_KEY),
        conf.get(LAKEFS_CONF_JOB_SOURCE_NAME_KEY, "input_format")
      )
    )
    if (StringUtils.isBlank(storageNamespace)) {
      storageNamespace = apiClient.getStorageNamespace(repoName, StorageClientType.HadoopFS)
    }
    val namespaceURI = URI.create(storageNamespace)
    val fs = FileSystem.get(namespaceURI, conf)
    fs.getStatus(new Path(namespaceURI)) // Will throw an exception if namespace doesn't exist

    val metadataURI =
      namespaceURI.resolve(if (storageNamespace.endsWith("/")) "_lakefs" else "/_lakefs")
    val metadataPath = new Path(metadataURI)
    if (!fs.exists(metadataPath)) {
      return ListBuffer.empty[InputSplit].asJava
    }

    val splits = new ListBuffer[InputSplit]()
    val it = fs.listFiles(metadataPath, false)
    while (it.hasNext) {
      val file = it.next()
      splits += new GravelerSplit(
        file.getPath,
        file.getPath.getName,
        file.getLen,
        false
      )
    }
    logger.debug(s"Returning ${splits.size} splits")
    splits.asJava
  }
}
