package io.treeverse.clients

import io.treeverse.clients.catalog.Entry
import io.treeverse.clients.committed.RangeData
import io.treeverse.clients.{Range => LakeFSRange}
import org.apache.commons.codec.binary.Hex
import org.rocksdb._

import java.io.{ByteArrayInputStream, DataInputStream, IOException}
import scala.collection.mutable.ListBuffer

object SSTableReader {

  private trait DataHandler[T] {
    def handle(key: Array[Byte], identity: Array[Byte], data: Array[Byte]): T
  }

  private class RangeDataHandler extends SSTableReader.DataHandler[io.treeverse.clients.Range] {
    override def handle(key: Array[Byte], identity: Array[Byte], data: Array[Byte]): LakeFSRange = try new LakeFSRange(new String(key), new String(identity), RangeData.parseFrom(data))
    catch {
      case e: IOException =>
        e.printStackTrace()
        null
    }
  }

  private class EntryDataHandler extends SSTableReader.DataHandler[EntryRecord] {
    override def handle(key: Array[Byte], identity: Array[Byte], data: Array[Byte]): EntryRecord = try new EntryRecord(new String(key), Hex.encodeHexString(identity), Entry.parseFrom(data))
    catch {
      case e: IOException =>
        e.printStackTrace()
        null
    }
  }

}

class SSTableReader() {
  private val reader = new SstFileReader(new Options().setCompressionType(CompressionType.SNAPPY_COMPRESSION))
  RocksDB.loadLibrary()

  @throws[RocksDBException]
  @throws[IOException]
  def getData[T](sstableFile: String, handler: SSTableReader.DataHandler[T], expectedType: String): Seq[T] = {
    reader.open(sstableFile)
    var result = new ListBuffer[T]()
    val it = reader.newIterator(new ReadOptions)
    val props = reader.getTableProperties.getUserCollectedProperties
    if (expectedType != props.get("type")) {
      throw new RuntimeException(String.format("expected property type to be '%s'. got '%s'", expectedType, props.get("type")))
    }
    it.seekToFirst()
    while (it.isValid) {
      val inputStream = new ByteArrayInputStream(it.value)
      val s = new DataInputStream(inputStream)
      val identityLength = VarInt.readSignedVarLong(s)
      val id = inputStream.readNBytes(identityLength.toInt)
      val dataLength = VarInt.readSignedVarLong(s)
      val data = inputStream.readNBytes(dataLength.toInt)
      result += handler.handle(it.key, id, data)
      it.next()
    }
    result.toList
  }

  @throws[RocksDBException]
  @throws[IOException]
  def getRanges(sstableFile: String): Seq[LakeFSRange] = getData(sstableFile, new SSTableReader.RangeDataHandler, "metaranges")

  @throws[RocksDBException]
  @throws[IOException]
  def getEntries(sstableFile: String): Seq[EntryRecord] = getData(sstableFile, new SSTableReader.EntryDataHandler, "ranges")
}
