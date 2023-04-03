package io.treeverse.clients

import io.treeverse.jpebble.{BlockParser, BlockReadableFile, Entry => PebbleEntry}
import com.google.protobuf.CodedInputStream
import io.treeverse.lakefs.catalog.Entry
import io.treeverse.lakefs.graveler.committed.RangeData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.{ByteArrayInputStream, Closeable, DataInputStream, File}

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
  private def copyToLocal(configuration: Configuration, url: String) = {
    val p = new Path(url)
    val fs = p.getFileSystem(configuration)
    val localFile = File.createTempFile("lakefs.", ".sstable")
    // Cleanup the local file - using the same technic as other data sources:
    // https://github.com/apache/spark/blob/c0b1735c0bfeb1ff645d146e262d7ccd036a590e/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/text/TextFileFormat.scala#L123
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => localFile.delete()))

    // TODO(#2403): Implement a BlockReadable on top of AWS
    //     FSDataInputStream, use that.
    fs.copyToLocalFile(false, p, new Path(localFile.getAbsolutePath), true)
    localFile
  }

  def forMetaRange(
      configuration: Configuration,
      metaRangeURL: String,
      dontOwn: Boolean = false
  ): SSTableReader[RangeData] = {
    val localFile: File = copyToLocal(configuration, metaRangeURL)
    val ret = new SSTableReader(localFile, RangeData.messageCompanion, dontOwn)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => ret.close()))
    ret
  }

  def forRange(
      configuration: Configuration,
      rangeURL: String,
      dontOwn: Boolean = false
  ): SSTableReader[Entry] = {
    val localFile: File = copyToLocal(configuration, rangeURL)
    val ret = new SSTableReader(localFile, Entry.messageCompanion, dontOwn)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => ret.close()))
    ret
  }
}

class SSTableReader[Proto <: GeneratedMessage with scalapb.Message[Proto]] private (
    val file: java.io.File,
    val companion: GeneratedMessageCompanion[Proto],
    val dontOwn: Boolean = false
) extends Closeable {
  private val fp = new java.io.RandomAccessFile(file, "r")
  private val reader = new BlockReadableFile(fp)

  def this(sstableFilename: String, companion: GeneratedMessageCompanion[Proto], dontOwn: Boolean) =
    this(new java.io.File(sstableFilename), companion, dontOwn)

  def close(): Unit = {
    fp.close()
    if (!dontOwn) {
      file.delete()
    }
  }

  def getProperties: Map[String, Array[Byte]] = {
    val bytes = reader.iterate(reader.length - BlockParser.footerLength, BlockParser.footerLength)
    val footer = BlockParser.readFooter(bytes)
    BlockParser.readProperties(reader, footer).map(kv => new String(kv._1.toArray) -> kv._2)
  }

  def newIterator(): SSTableIterator[Proto] = {
    val it = BlockParser.entryIterator(reader)
    new SSTableIterator(it, this, companion)
  }
}
