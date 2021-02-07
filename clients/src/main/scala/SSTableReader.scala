import catalog.catalog.Entry
import committed.committed.RangeData
import org.rocksdb._

import java.io.ByteArrayInputStream
import java.io.DataInput
import java.io.DataInputStream
import java.io.IOException
import java.util
import java.util.{ArrayList, List, Map}

object SSTableReader {

  private trait DataHandler[T] {
    def handle(key: Array[Byte], identity: Array[Byte], data: Array[Byte]): T
  }

  private class RangeDataHandler extends SSTableReader.DataHandler[Range] {
    override def handle(key: Array[Byte], identity: Array[Byte], data: Array[Byte]): Range = try new Range(new String(key), new String(identity), RangeData.parseFrom(data))
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
  def getData[T](sstableFile: String, handler: SSTableReader.DataHandler[T], expectedType: String): util.List[T] = {
    reader.open(sstableFile)
    val result = new util.ArrayList[T](reader.getTableProperties.getNumEntries.toInt)
    val it = reader.newIterator(new ReadOptions)
    val props = reader.getTableProperties.getUserCollectedProperties
    if (!(expectedType == props.get("type"))) throw new RuntimeException(String.format("expected property type to be '%s'. got '%s'", expectedType, props.get("type")))
    it.seekToFirst()
    while ( {
      it.isValid
    }) {
      val inputStream = new ByteArrayInputStream(it.value)
      val s = new DataInputStream(inputStream)
      val identityLength = VarInt.readSignedVarLong(s)
      val id = inputStream.readNBytes(identityLength.toInt)
      val dataLength = VarInt.readSignedVarLong(s)
      val data = inputStream.readNBytes(dataLength.toInt)
      result.add(handler.handle(it.key, id, data))
      it.next()
    }
    result
  }

  @throws[RocksDBException]
  @throws[IOException]
  def getRanges(sstableFile: String): util.List[Range] = getData(sstableFile, new SSTableReader.RangeDataHandler, "metaranges")

  @throws[RocksDBException]
  @throws[IOException]
  def getEntries(sstableFile: String): util.List[EntryRecord] = getData(sstableFile, new SSTableReader.EntryDataHandler, "ranges")
}
