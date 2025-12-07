package io.treeverse.clients

import io.treeverse.jpebble.{BlockParser, BlockReadable, BlockReadableFile, Entry => PebbleEntry}
import com.google.protobuf.CodedInputStream
import io.treeverse.lakefs.catalog.Entry
import io.treeverse.lakefs.graveler.committed.RangeData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import org.slf4j.{Logger, LoggerFactory}
import java.io.{ByteArrayInputStream, Closeable, DataInputStream}

class Item[T](val key: Array[Byte], val id: Array[Byte], val message: T)

private object local {
  def readNBytes(s: DataInputStream, n: Int): Array[Byte] = {
    val ret = new Array[Byte](n)
    s.readFully(ret)
    ret
  }
}

class SSTableIterator[Proto <: GeneratedMessage with scalapb.Message[Proto]](
    val it: Iterator[PebbleEntry],
    val resource: Closeable,
    val companion: GeneratedMessageCompanion[Proto]
) extends Iterator[Item[Proto]] {
  // TODO(ariels): explicitly make it closeable, and figure out how to close it when used by
  //     Spark.
  override def hasNext: Boolean = {
    val ret = it.hasNext
    if (!ret) {
      resource.close()
    }
    ret
  }

  override def next(): Item[Proto] = {
    val entry = it.next
    val bais = new ByteArrayInputStream(entry.value)
    val key = entry.key
    val dis = new DataInputStream(bais)
    val identityLength = VarInt.readSignedVarLong(dis)
    val id = local.readNBytes(dis, identityLength.toInt)
    val dataLength = VarInt.readSignedVarLong(dis)
    val data = local.readNBytes(dis, dataLength.toInt)
    // TODO(ariels): Error if dis is not exactly at end?  (But then cannot add more fields in
    //     future, sigh...)

    val dataStream = CodedInputStream.newInstance(data)
    val message = companion.parseFrom(dataStream)

    // TODO (johnnyaug) validate item is of the expected type - metarange/range
    dataStream.checkLastTagWas(0)

    new Item(key, id, message)
  }
}

object SSTableReader {
  def forMetaRange(
      configuration: Configuration,
      metaRangeURL: String,
      own: Boolean = true
  ): SSTableReader[RangeData] = {
    val p = new Path(metaRangeURL)
    val fs = p.getFileSystem(configuration)
    val fileLength = fs.getFileStatus(p).getLen
    val reader = new HadoopBlockReadable(fs, p, fileLength)
    val ret = new SSTableReader(reader, RangeData.messageCompanion)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => ret.close()))
    ret
  }

  def forRange(
      configuration: Configuration,
      rangeURL: String,
      own: Boolean = true
  ): SSTableReader[Entry] = {
    val p = new Path(rangeURL)
    val fs = p.getFileSystem(configuration)
    val fileLength = fs.getFileStatus(p).getLen
    val reader = new HadoopBlockReadable(fs, p, fileLength)
    val ret = new SSTableReader(reader, Entry.messageCompanion)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => ret.close()))
    ret
  }
}

class SSTableReader[Proto <: GeneratedMessage with scalapb.Message[Proto]] (
    val reader: BlockReadable,
    val companion: GeneratedMessageCompanion[Proto],
    val closeAction: () => Unit = () => ()
) extends Closeable {
  private val logger: Logger = LoggerFactory.getLogger(getClass.toString)

  def this(file: java.io.File, companion: GeneratedMessageCompanion[Proto], own: Boolean) = {
    this(new BlockReadableFile(new java.io.RandomAccessFile(file, "r")), companion, () => {
      if (own) {
        try {
          file.delete()
        } catch {
          case e: Exception =>
            LoggerFactory.getLogger(classOf[SSTableReader[Proto]].toString).warn(s"delete owned file ${file.getName} (keep going): $e")
        }
      }
    })
  }

  def this(sstableFilename: String, companion: GeneratedMessageCompanion[Proto], own: Boolean) =
    this(new java.io.File(sstableFilename), companion, own)

  def close(): Unit = {
    if (reader.isInstanceOf[Closeable]) {
      try {
        reader.asInstanceOf[Closeable].close()
      } catch {
        case e: Exception =>
          logger.warn(s"close reader (keep going): $e")
      }
    }
    closeAction()
  }

  def getProperties: Map[String, Array[Byte]] = {
    val bytes = reader.iterate(reader.length - BlockParser.footerLength, BlockParser.footerLength)
    val footer = BlockParser.readFooter(bytes)
    BlockParser.readProperties(reader, footer).map(kv => new String(kv._1.toArray) -> kv._2)
  }

  def newIterator(): Iterator[Item[Proto]] = {
    val it = BlockParser.entryIterator(reader)
    new SSTableIterator(it, this, companion)
  }
}