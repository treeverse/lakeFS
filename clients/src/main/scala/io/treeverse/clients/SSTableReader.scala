package io.treeverse.clients

import io.treeverse.clients.catalog.Entry
import io.treeverse.clients.committed.RangeData
import io.treeverse.clients.{Range => LakeFSRange}
import org.rocksdb.{SstFileReader, _}

import java.io.{ByteArrayInputStream, DataInputStream, IOException}

class SSTableItem(val key: Array[Byte], val id: Array[Byte], val data: Array[Byte])

class SSTableIterator(val it: SstFileReaderIterator) extends Iterator[SSTableItem] {
  it.seekToFirst()

  override def hasNext: Boolean = it.isValid

  override def next(): SSTableItem = {
    val bais = new ByteArrayInputStream(it.value)
    val key = it.key()
    val dis = new DataInputStream(bais)
    val identityLength = VarInt.readSignedVarLong(dis)
    val id = dis.readNBytes(identityLength.toInt)
    val dataLength = VarInt.readSignedVarLong(dis)
    val data = dis.readNBytes(dataLength.toInt)
    it.next()
    new SSTableItem(key, id, data)
  }
}

object SSTableReader {
  RocksDB.loadLibrary()
}

class SSTableReader() {
  private val reader = new SstFileReader(new Options)

  @throws[RocksDBException]
  @throws[IOException]
  def getData[T](sstableFile: String, expectedType: String): SSTableIterator = {
    reader.open(sstableFile)
    val props = reader.getTableProperties.getUserCollectedProperties
    if (expectedType != props.get("type")) {
      throw new RuntimeException(String.format("expected property type to be '%s'. got '%s'", expectedType, props.get("type")))
    }
    new SSTableIterator(reader.newIterator(new ReadOptions))
  }

  @throws[RocksDBException]
  @throws[IOException]
  def getRanges(sstableFile: String): Seq[LakeFSRange] = {
    getData(sstableFile, "metaranges")
      .map(tableItem => new LakeFSRange(tableItem.key, tableItem.id, RangeData.parseFrom(tableItem.data)))
      .toSeq
  }

  @throws[RocksDBException]
  @throws[IOException]
  def getEntries(sstableFile: String): Seq[EntryRecord] = {
    getData(sstableFile, "ranges")
      .map(tableItem => new EntryRecord(tableItem.key, tableItem.id, Entry.parseFrom(tableItem.data)))
      .toSeq
  }
}
