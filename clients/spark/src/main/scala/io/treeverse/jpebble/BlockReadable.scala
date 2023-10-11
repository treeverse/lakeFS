package io.treeverse.jpebble

import java.io.{Closeable, RandomAccessFile}

/** Interface for reading blocks.  This is for reading storage with some
 *  random-access capabilities.
 */
trait BlockReadable {
  def length: Long

  /** Return a new block.
   *
   *  @param offset from start.
   *  @param size to read into.
   */
  def readBlock(offset: Long, size: Long): IndexedBytes

  /** Return an iterator on a block.
   */
  def iterate(offset: Long, size: Long): Iterator[Byte] =
    readBlock(offset, size).iterator
}

class BlockReadableFile(private val in: RandomAccessFile) extends BlockReadable with Closeable {
  if (in == null) {
    throw new IllegalArgumentException("null file");
  }

  lazy val inSize = in.length() // Compute once, the file should anyway be immutable!

  override def length = inSize

  override def readBlock(offset: Long, size: Long) = {
    // TODO(ariels): Cache return values - our RandomAccessFiles are reusable!
    val buf = new Array[Byte](size.toInt)
    in.synchronized {
      in.seek(offset)
      val bytesRead = in.read(buf)
      if (bytesRead != size) {
        throw new java.io.IOException(s"Premature EOF $bytesRead < $size bytes")
      }
    }
    IndexedBytes.create(buf)
  }

  override def close() = in.close()
}
