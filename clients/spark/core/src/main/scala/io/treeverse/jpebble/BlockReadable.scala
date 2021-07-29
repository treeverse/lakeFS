package io.treeverse.jpebble

import java.nio.channels.FileChannel

/**
 * Interface for reading blocks.  This is for reading storage with some
 * random-access capabilities.
 */
trait BlockReadable {
  /**
   * Return a new block.
   *
   * @param offset from start.
   * @param buffer to read into.
   */
  def readBlock(offset: Long, size: Long): IndexedBytes

  /**
   * Return an iterator on a block.
   */
  def iterate(offset: Long, size: Long): Iterator[Byte] =
    readBlock(offset, size).iterator
}

class BlockReadableFileChannel(private val in: FileChannel) extends BlockReadable {
  override def readBlock(offset: Long, size: Long) =
    // TODO(ariels): Cache return values - readonly mmaps are reusable
    IndexedBytes.create(in.map(FileChannel.MapMode.READ_ONLY, offset, size))
}
