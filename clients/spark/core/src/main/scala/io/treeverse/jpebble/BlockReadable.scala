package io.treeverse.jpebble

import java.nio.channels.FileChannel
import java.io.Closeable

/** Interface for reading blocks.  This is for reading storage with some
 *  random-access capabilities.
 */
trait BlockReadable {
  def length: Long

  /** Return a new block.
   *
   *  @param offset from start.
   *  @param buffer to read into.
   */
  def readBlock(offset: Long, size: Long): IndexedBytes

  /** Return an iterator on a block.
   */
  def iterate(offset: Long, size: Long): Iterator[Byte] =
    readBlock(offset, size).iterator
}

class BlockReadableFileChannel(private val in: FileChannel) extends BlockReadable with Closeable {
  val size = in.size() // Compute once, the file should anyway be immutable!

  override def length: Long = size

  override def readBlock(offset: Long, size: Long) =
    // TODO(ariels): Cache return values - readonly mmaps are reusable
    IndexedBytes.create(in.map(FileChannel.MapMode.READ_ONLY, offset, size))

  override def close() = in.close()
}
