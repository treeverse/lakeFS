package io.treeverse.jpebble

import org.xerial.snappy.Snappy

import java.io.IOException
import java.util.zip.Checksum;
import org.xerial.snappy.{PureJavaCrc32C => CRC32C}

// The Block-Based RocksDB SSTable format is described in
// https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format

case class BlockHandle(offset: Long, size: Long) {
  override def toString(): String = "BH[%08x + %04x]".format(offset, size)
}

case class IndexBlockHandles(metaIndex: BlockHandle, index: BlockHandle)

class BadFileFormatException(msg: String, cause: Throwable = null) extends IOException(msg, cause)

/** A wrapper for Iterator that counts calls to next.
 *
 *  Iterator.zipWithIndex is not a good substitute, because it requires an
 *  extra call to Iterator.next -- which fails if the iterator is done and
 *  will also prevent the next parser from running..
 */
class CountedIterator[T](it: Iterator[T]) extends Iterator[T] {
  var count = 0

  def hasNext = it.hasNext
  def next(): T = {
    count += 1
    it.next()
  }
}

class DebuggingIterator(it: Iterator[Byte]) extends Iterator[Byte] {
  def hasNext = {
    val hn = it.hasNext
    if (!hn) { Console.out.println("[DEBUG] end of iteration") }
    hn
  }

  def next(): Byte = {
    val n = it.next()
    Console.out.println(s"[DEBUG] next ${n} ${n.asInstanceOf[Char]}")
    n
  }
}

object Binary {

  /** @return a readable string for bytes: keeps ASCII bytes, turns
   *     everything else into a hex escapes "\xab".
   */
  def readable(bytes: Seq[Byte]): String =
    bytes.foldLeft("")((s: String, b: Byte) =>
      s + (if (32 <= b && b < 127) b.toChar.toString else f"\\x$b%02x")
    )
}

case class Entry(key: Array[Byte], value: Array[Byte]) {
  import Binary.readable
  override def toString() = s"${readable(key)} -> ${readable(value)}"
}

/** Iterator over elements of an index block.  No "random" (O(log n)) access
 *  provided, just iteration over keys and values.
 */
class DataBlockIterator(private val it: Iterator[Byte]) extends Iterator[Entry] {
  // Format is documented in the source code,
  // https://github.com/facebook/rocksdb/blob/74b7c0d24997e12482105c09b47c7223e7b75b96/table/block_based/block_builder.cc#L10-L32
  //
  // Specifically this quote:
  //
  // An entry for a particular key-value pair has the form:
  //     shared_bytes: varint32
  //     unshared_bytes: varint32
  //     value_length: varint32
  //     key_delta: char[unshared_bytes]
  //     value: char[value_length]
  // shared_bytes == 0 for restart points.

  var lastKey = Array[Byte]()

  override def hasNext = it.hasNext

  override def next(): Entry = {
    val sharedBytesSize = BlockParser.readUnsignedVarLong(it)
    val unsharedBytesSize = BlockParser.readUnsignedVarLong(it)
    val valueSize = BlockParser.readUnsignedVarLong(it)

    // BUG(ariels): no support for Long sizes here.  Luckily that is not
    //     very useful in an SSTable (but possible).
    val keySharedPrefix = lastKey.slice(0, sharedBytesSize.toInt)
    val keyUnshared = BlockParser.readBytes(it, unsharedBytesSize).toArray
    val value = BlockParser.readBytes(it, valueSize).toArray
    lastKey = (keySharedPrefix ++ keyUnshared).toArray
    Entry(lastKey, value)
  }

  /** Convert (remainder of iterator) to a Map.  Arrays are great for
   *  entries, less so for Map keys -- use Seq keys to allow value lookups.
   */
  def toMap: Map[Seq[Byte], Array[Byte]] = this.map({ case Entry(k, v) => (k.toSeq, v) }).toMap
}

object BlockParser {
  // The CockroachDB "pebbles" footer length, excluding the checksum type
  // but including a 4-byte checksum.
  val footerLength = 48 + 4
  // Support (only) the non-legacy format.  It is compatible with reading
  // the legacy footer and identifying this different footer magic.
  val footerMagic = Seq(0xf7, 0xcf, 0xf4, 0x85, 0xb7, 0x41, 0xe2, 0x88).map(_.toByte)

  val blockTrailerLen = 1 + 4

  val COMPRESSION_BLOCK_TYPE_NONE = 0
  val COMPRESSION_BLOCK_TYPE_SNAPPY = 1

  val INDEX_TYPE_KEY = "rocksdb.block.based.table.index.type".getBytes
  val INDEX_TYPE_TWO_LEVEL = 2

  def update(checksum: Checksum, buf: Array[Byte], offset: Int, length: Int) {
    checksum.update(buf.array, offset, length)
  }

  def readEnd(bytes: Iterator[Byte]) =
    if (bytes.hasNext) throw new BadFileFormatException("Input too long")

  def readMagic(bytes: Iterator[Byte]) = {
    val magic = bytes.take(footerMagic.length).toArray
    if (magic.size < footerMagic.length) {
      throw new BadFileFormatException(
        s"Bad magic ${magic.map("%02x".format(_)).mkString(" ")}: too short"
      )
    }
    val isMatch = magic
      .zip(BlockParser.footerMagic)
      .filter({ case ((a, b)) => a != b })
      .isEmpty
    if (!isMatch) {
      throw new BadFileFormatException(
        s"Bad magic ${magic.map("%02x".format(_)).mkString(" ")}: wrong bytes"
      )
    }
  }

  /** Return an int32 with (fixed-width, 4 byte) little-endian encoding from bytes.
   */
  def readInt32(bytes: Iterator[Byte]): Int =
    // RocksDB format is little-endian.  And Scala applies functions
    // left-to-right, so the first Seq.next call really executes first.
    //
    // (To get "unsigned" (-ish) byte values, take a bitwise AND with the
    // Int 0xFF.  That widens to Int but prevents sign extension.  Languages
    // without unsigned integer types are so much fun.)
    ((bytes.next() & 0xff) + 256L * (
      (bytes.next() & 0xff) + 256L * ((bytes.next() & 0xff) + 256L * (bytes.next() & 0xff))
    )).toInt

  def readUnsignedVarLong(bytes: Iterator[Byte]) = {
    val (continuedBytes, rest) = bytes.span((b: Byte) => (b & 0x80L) != 0)
    val (i, v) = continuedBytes
      .foldLeft((0, 0L))(
        { case ((i, v), b) => (i + 7, v | (b & 0x7f).toLong << i) }
      )
    if (i > 63) throw new BadFileFormatException("Variable length quantity is too long")
    v | (rest.next.toLong << i)
  }

  def readSignedVarLong(bytes: Iterator[Byte]): Long = {
    val raw = readUnsignedVarLong(bytes)
    // This undoes the trick in writeSignedVarLong()
    val temp = (((raw << 63) >> 63) ^ raw) >> 1
    // This extra step lets us deal with the largest signed values by treating
    // negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    temp ^ (raw & (1L << 63))
  }

  // BUG(ariels): no support for Long sizes.  Not very useful in an SSTable (but possible).
  def readBytes(bytes: Iterator[Byte], size: Long): Seq[Byte] = bytes.take(size.toInt).toSeq

  def readBlockHandle(bytes: Iterator[Byte]) =
    new BlockHandle(readUnsignedVarLong(bytes), readUnsignedVarLong(bytes))

  def readFooter(bytes: Iterator[Byte]): IndexBlockHandles = {
    val countedBytes = new CountedIterator(bytes)
    val ret = new IndexBlockHandles(readBlockHandle(countedBytes), readBlockHandle(countedBytes))
    val skip = BlockParser.footerLength - countedBytes.count - footerMagic.length
    if (skip < 0) {
      throw new BadFileFormatException("[I] Footer overflow (bad varint parser?)")
    }

    val after = bytes.drop(skip)

    readMagic(after)

    ret
  }

  /** Mix bits of CRC32C to match its use in RocksDB SSTables.  (That format
   *  includes CRCs inside checksummed data, meaning further CRCs of that
   *  block can fail to detect anything; defining this mixing protects that
   *  protocol.  We need to follow the format regardless of whether or not
   *  using CRCs like this is justified!)
   */
  def fixupCRC(crc: Int): Int = (crc >>> 15 | crc << 17) + 0xa282ead8

  /** Verify the block checksum and return the sequence of its contents.
   *  block should be the entire contents of a BlockHandle plus
   *  blockTrailerLen (5) bytes.
   *
   *  Uses a ByteBuffer so we can use whatever efficient CRC32C
   *  implementation is available on the JVM.
   *
   *  TODO(ariels): decompression.
   */
  def startBlockParse(block: IndexedBytes): IndexedBytes = {
    val crc = new CRC32C()
    update(crc, block.bytes, block.from, block.size - blockTrailerLen + 1)
    val computedCRC = fixupCRC(crc.getValue().toInt)
    val expectedCRC = readInt32(
      block.slice(block.size - blockTrailerLen + 1, block.size).iterator
    )
    if (computedCRC != expectedCRC) {
      throw new BadFileFormatException(
        "Bad CRC got %08x != stored %08x".format(computedCRC, expectedCRC)
      )
    }
    val compressionType = block(block.size - blockTrailerLen)
    val data = block.slice(0, block.size - blockTrailerLen)
    compressionType match {
      case COMPRESSION_BLOCK_TYPE_NONE => data
      case COMPRESSION_BLOCK_TYPE_SNAPPY => {
        val dataBytes = data.bytes
        try {
          val uncompressedLength = Snappy.uncompressedLength(dataBytes, data.from, data.size)
          val uncompressed = new Array[Byte](uncompressedLength)
          Snappy.uncompress(dataBytes, data.from, data.size, uncompressed, 0)
          IndexedBytes.create(uncompressed)
        } catch {
          case e: IOException =>
            throw new BadFileFormatException(s"Bad Snappy-compressed data", e)
        }
      }
      case _ => throw new BadFileFormatException(s"Unknown compression type $compressionType")
    }
  }

  def parseDataBlock(block: IndexedBytes): DataBlockIterator = {
    // Ignore block trailer, documented in the source code
    // https://github.com/facebook/rocksdb/blob/74b7c0d24997e12482105c09b47c7223e7b75b96/table/block_based/block_builder.cc#L29-L32
    //
    // by:
    //
    // The trailer of the block has the form:
    //     restarts: uint32[num_restarts]
    //     num_restarts: uint32
    // restarts[i] contains the offset within the block of the ith restart point.
    val numRestarts = readInt32(block.slice(block.size - 4, block.size).iterator)
    val blockWithoutTrailer = block.slice(0, block.size - 4 * (numRestarts + 1))
    new DataBlockIterator(blockWithoutTrailer.iterator)
  }

  def readProperties(
      file: BlockReadable,
      footer: IndexBlockHandles
  ): Map[Seq[Byte], Array[Byte]] = {
    val metaIndex = {
      val bytes =
        file.readBlock(footer.metaIndex.offset, footer.metaIndex.size + BlockParser.blockTrailerLen)
      val block = BlockParser.startBlockParse(bytes)
      BlockParser.parseDataBlock(block).toMap
    }

    val propBHIt = metaIndex("rocksdb.properties".getBytes).iterator
    val propBH = readBlockHandle(propBHIt)
    readEnd(propBHIt)

    {
      val bytes = file.readBlock(propBH.offset, propBH.size + BlockParser.blockTrailerLen)
      val block = BlockParser.startBlockParse(bytes)
      BlockParser.parseDataBlock(block).toMap
    }
  }

  /** @return Iterator over all SSTable entries
   */
  def entryIterator(in: BlockReadable): Iterator[Entry] = {
    if (in.length < BlockParser.footerLength) {
      throw new BadFileFormatException(
        s"Block of length ${in.length} too short: not enough footer bytes"
      )
    }
    val bytes = in.iterate(in.length - BlockParser.footerLength, BlockParser.footerLength)
    val footer = BlockParser.readFooter(bytes)
    BlockParser.readEnd(bytes)

    val blockIndexType = {
      val props = BlockParser.readProperties(in, footer)
      val typeIt = props(BlockParser.INDEX_TYPE_KEY).iterator
      val typ = BlockParser.readInt32(typeIt)
      BlockParser.readEnd(typeIt)
      typ
    }

    val indexIt = {
      val bytes = in.readBlock(footer.index.offset, footer.index.size + BlockParser.blockTrailerLen)
      val block = BlockParser.startBlockParse(bytes)
      BlockParser.parseDataBlock(block)
    }

    val index2It =
      if (blockIndexType == INDEX_TYPE_TWO_LEVEL) // TODO(ariels): == or & ?
        indexIt.flatMap((data) => {
          val it = data.value.iterator
          val bh = BlockParser.readBlockHandle(it)
          BlockParser.readEnd(it)

          val bytes = in.readBlock(bh.offset, bh.size + BlockParser.blockTrailerLen)
          val block = BlockParser.startBlockParse(bytes)
          BlockParser.parseDataBlock(block)
        })
      else
        indexIt

    val entryIt = index2It.flatMap((data) => {
      // Ignore separating key: for iterating over all the value, only the
      // blockhandle in the value is important.
      val it = data.value.iterator
      val bh = BlockParser.readBlockHandle(it)
      BlockParser.readEnd(it)

      val bytes = in.readBlock(bh.offset, bh.size + BlockParser.blockTrailerLen)
      val block = BlockParser.startBlockParse(bytes)
      BlockParser.parseDataBlock(block)
    })

    entryIt.map(stripInternalKey)
  }

  /** RocksDB adds 8 bytes at the end of the key of every user item of data.
   *  These bytes indicate a version number and a tombstone, both used to
   *  mutate SSTables.  lakeFS uses PebbleDB SSTables for purely immutable
   *  storage, so these 8 bytes are not needed.  Just strip them away to
   *  retain the existing user key.
   *
   *  @return entry with its "internal" key stripped to become a user key.
   */
  private def stripInternalKey(entry: Entry) =
    new Entry(entry.key.slice(0, entry.key.length - 8), entry.value)
}
