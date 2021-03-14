package io.treeverse.clients

import com.google.protobuf.CodedInputStream
import org.rocksdb.{SstFileReader, _}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.io.{ByteArrayInputStream, Closeable, DataInputStream}
import scala.collection.JavaConverters._

class Item[T](val key: Array[Byte], val id: Array[Byte], val message: T)

private object local {
  def readNBytes(s: DataInputStream, n: Int): Array[Byte] = {
    val ret = new Array[Byte](n)
    s.readFully(ret)
    ret
  }
}

class SSTableIterator[Proto <: GeneratedMessage with scalapb.Message[Proto]](val it: SstFileReaderIterator, companion: GeneratedMessageCompanion[Proto]) extends Iterator[Item[Proto]] with Closeable {
  // TODO(ariels): explicitly make it closeable, and figure out how to close it when used by
  //     Spark.
  override def hasNext: Boolean = it.isValid

  override def close() : Unit = it.close()

  override def next(): Item[Proto] = {
    val bais = new ByteArrayInputStream(it.value)
    val key = it.key()
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
    it.next()

    new Item(key, id, message)
  }
}

object SSTableReader {
  RocksDB.loadLibrary()
}

class SSTableReader[Proto <: GeneratedMessage with scalapb.Message[Proto]](sstableFile: String, companion: GeneratedMessageCompanion[Proto]) extends Closeable {
  private val options = new Options
  private val reader = new SstFileReader(options)
  private val readOptions = new ReadOptions
  reader.open(sstableFile)

  def close(): Unit = {
    reader.close()
    options.close()
    readOptions.close()
  }

  def getMetadata: Map[String, String] = reader.getTableProperties.getUserCollectedProperties.asScala.toMap

  def newIterator(): SSTableIterator[Proto] = {
    val it = reader.newIterator(readOptions)
    it.seekToFirst()
    new SSTableIterator(it, companion)
  }
}
