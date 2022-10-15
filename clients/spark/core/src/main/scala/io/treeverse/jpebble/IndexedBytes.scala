package io.treeverse.jpebble

object IndexedBytes {
  def create(buf: Array[Byte]): IndexedBytes = new BufferIndexedBytes(buf, 0, buf.size)
}

trait IndexedBytes {
  // Limit to 2GiB, because Java arrays have limited size, because Java.
  def size: Int

  /** @return underlying array of bytes. */
  def bytes: Array[Byte]

  /** @return start offset of this in bytes. */
  def from: Int

  def slice(offset: Int, size: Int): IndexedBytes
  def iterator: Iterator[Byte]
  def apply(i: Int): Byte
}

/** Iterator over a BufferIndexedBytes
 */
class BufferIterator(private val buf: BufferIndexedBytes) extends Iterator[Byte] {
  var index = 0
  override def hasNext = index < buf.size
  override def next(): Byte = {
    try {
      val ret = buf(index)
      index += 1
      ret
    } catch {
      case e: (IndexOutOfBoundsException) =>
        throw new java.util.NoSuchElementException().initCause(e)
    }
  }

  override def take(n: Int): Iterator[Byte] = {
    val ret = buf.sliceView(index, index + n).iterator
    index += n
    ret
  }
}

/** IndexedBytes running on an immutable array of bytes on the range
 *  [offset, offset+length).  After calling, change *nothing* in buf.
 */
class BufferIndexedBytes(
    private val buf: Array[Byte],
    private val offset: Int,
    private val length: Int
) extends IndexedBytes {
  def size = length
  def bytes = buf
  def from = offset

  if (offset + length > buf.size) {
    throw new IndexOutOfBoundsException(
      s"Cannot create buffer on [$offset, ${offset + length}) from array of size ${buf.size}"
    )
  }

  override def slice(start: Int, end: Int) = {
    if (start < 0 || end < 0) {
      throw new IndexOutOfBoundsException(s"Cannot expand slice before start to [$start, $end)")
    }
    if (end > size) {
      throw new IndexOutOfBoundsException(
        s"Cannot expand slice of size $size after end to [$start, $end)"
      )
    }
    new BufferIndexedBytes(buf, offset + start, end - start)
  }

  def sliceView(start: Int, end: Int) = {
    if (start < 0 || end < 0) {
      throw new IndexOutOfBoundsException(
        s"Cannot expand slice of size $size after end to [$start, $end)"
      )
    }
    if (end > size) {
      throw new IndexOutOfBoundsException(
        s"Cannot expand slice of size $size after end to [$start, $end)"
      )
    }
    buf.view.slice(offset + start, offset + end)
  }

  override def iterator = new BufferIterator(this)

  override def apply(i: Int) = buf(offset + i)
}
