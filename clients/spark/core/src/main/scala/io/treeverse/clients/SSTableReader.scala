package io.treeverse.clients

import io.treeverse.jpebble.{BlockParser, BlockReadableFile, Entry => PebbleEntry}
import com.google.protobuf.CodedInputStream
import io.treeverse.lakefs.catalog.Entry
import io.treeverse.lakefs.graveler.committed.RangeData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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
    companion: GeneratedMessageCompanion[Proto]
) extends Iterator[Item[Proto]] {
  // TODO(ariels): explicitly make it closeable, and figure out how to close it when used by
  //     Spark.
  override def hasNext: Boolean = it.hasNext

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
    localFile.deleteOnExit()
    // TODO(#2403): Implement a BlockReadable on top of AWS
    //     FSDataInputStream, use that.
    fs.copyToLocalFile(p, new Path(localFile.getAbsolutePath))
    localFile
  }

  def forMetaRange(configuration: Configuration, metaRangeURL: String) = {
    val localFile: File = copyToLocal(configuration, metaRangeURL)
    new SSTableReader(localFile, RangeData.messageCompanion)
  }

  def forRange(configuration: Configuration, rangeURL: String) = {
    val localFile: File = copyToLocal(configuration, rangeURL)
    new SSTableReader(localFile, Entry.messageCompanion)
  }
}

class SSTableReader[Proto <: GeneratedMessage with scalapb.Message[Proto]] private (
    fp: java.io.RandomAccessFile,
    companion: GeneratedMessageCompanion[Proto]
) extends Closeable {
  private val reader = new BlockReadableFile(fp)

  def this(sstableFilename: String, companion: GeneratedMessageCompanion[Proto]) =
    this(new java.io.RandomAccessFile(sstableFilename, "r"), companion)

  def this(sstableFile: File, companion: GeneratedMessageCompanion[Proto]) =
    this(new java.io.RandomAccessFile(sstableFile, "r"), companion)

  def close(): Unit = {
    fp.close()
  }

  def getProperties(): Map[String, Array[Byte]] = {
    val bytes = reader.iterate(reader.length - BlockParser.footerLength, BlockParser.footerLength)
    val footer = BlockParser.readFooter(bytes)
    BlockParser.readProperties(reader, footer).map(kv => (new String(kv._1.toArray) -> kv._2)).toMap
  }

  def newIterator(): SSTableIterator[Proto] = {
    val it = BlockParser.entryIterator(reader)
    new SSTableIterator(it, companion)
  }
}
