package io.treeverse.jpebble

import java.nio.{Buffer, ByteBuffer}

object IndexedBytes {
  def create(buf: ByteBuffer): IndexedBytes = new ByteBufferIndexedBytes(buf)
}

trait IndexedBytes {
  // Limit to 2GiB, because Java ByteBuffer has limited size, because Java.
  def size: Int
  def slice(offset: Int, size: Int): IndexedBytes
  def iterator: Iterator[Byte]
  def apply(i: Int): Byte
  def toByteBuffer: ByteBuffer = throw new UnsupportedOperationException(
    "toByteBuffer not implemented"
  )
}

/** Iterator over a ByteByffer.
 */
class ByteBufferIterator(private val buf: ByteBufferIndexedBytes) extends Iterator[Byte] {
  var index = 0
  override def hasNext = index < buf.size
  override def next(): Byte = {
    val ret =
      try {
        buf(index)
      } catch {
        case e: (IndexOutOfBoundsException) =>
          throw new java.util.NoSuchElementException().initCause(e)
      }
    index += 1
    ret
  }
}

/** IndexedBytes running on an immutable ("owned") ByteBufffer.  After
 *  calling, change *nothing* in buf, not even its position or limit.  (Or
 *  slice() it first to create a shallow copy, then you can change position
 *  and limit...)
 */
class ByteBufferIndexedBytes(private val buf: ByteBuffer) extends IndexedBytes {
  override def size = buf.limit() - buf.position()

  override def slice(start: Int, end: Int) = new ByteBufferIndexedBytes(
    // Hack around Java / Scala / Ubuntu / SBT / ??? madness: JDK changed
    // Buffer methods to be covariant in their return types in version 9.
    // Now Scala and/or Ubuntu compile based on the wrong JDK version, so
    // code emitted tries to access ByteBuffer.limit but the JVM has only
    // Buffer.limit.  Cast around that.
    //
    // Ref: https://stackoverflow.com/a/51223234/192263
    buf
      .slice()
      .asInstanceOf[Buffer]
      .limit(end)
      .position(start)
      .asInstanceOf[ByteBuffer]
  )

  override def iterator = new ByteBufferIterator(this)

  override def toByteBuffer = buf

  override def apply(i: Int) = toByteBuffer.get(i + buf.position())
}
