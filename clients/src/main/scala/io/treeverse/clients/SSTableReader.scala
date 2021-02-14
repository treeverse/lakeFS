package io.treeverse.clients

import com.google.protobuf.{CodedInputStream, Message}
import org.rocksdb.{SstFileReader, _}

import java.io.{ByteArrayInputStream, DataInputStream}

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

  def getData(sstableFile: String): SSTableIterator = {
    reader.open(sstableFile)
    new SSTableIterator(reader.newIterator(new ReadOptions))
  }

  def make[Proto <: Message](item: SSTableItem, messagePrototype: Proto): EntryRecord[Proto] = {
    val data = CodedInputStream.newInstance(item.data)
    val entry = new EntryRecord[Proto](
      item.key,
      item.id,
      messagePrototype.getParserForType.parseFrom(data).asInstanceOf[Proto],
    )
    // TODO (johnnyaug) validate item is of the expected type - metarange/range
    data.checkLastTagWas(0)
    entry
  }

  def get[Proto <: Message](sstableFile: String, messagePrototype: Proto): Seq[EntryRecord[Proto]] = getData(sstableFile)
    .map(make(_, messagePrototype))
    .toSeq

}
