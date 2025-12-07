package io.treeverse.clients

import java.io.Closeable
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import io.treeverse.jpebble.{BlockReadable, IndexedBytes}

class HadoopBlockReadable(
    private val fs: FileSystem,
    private val path: Path,
    private val fileLength: Long
) extends BlockReadable
    with Closeable {
  private val in: FSDataInputStream = fs.open(path)

  override def length: Long = fileLength

  override def readBlock(offset: Long, size: Long): IndexedBytes = {
    val buf = new Array[Byte](size.toInt)
    in.readFully(offset, buf)
    IndexedBytes.create(buf)
  }

  override def close(): Unit = in.close()
}
