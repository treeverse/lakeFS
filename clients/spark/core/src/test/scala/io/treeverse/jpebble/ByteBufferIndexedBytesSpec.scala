package io.treeverse.jpebble

import java.nio.ByteBuffer

import org.scalatest._
import matchers.should._
import funspec._

class ByteBufferIndexedBytesSpec extends AnyFunSpec with Matchers {
  val bytesSeq = Array[Byte](2, 3, 5, 7, 11, 13)

  describe("ByteBufferIndexedBytes") {
    it("returns bytes in buffer through iterator") {
      val bytes = IndexedBytes.create(ByteBuffer.wrap(bytesSeq)).iterator.toSeq

      bytes should contain theSameElementsInOrderAs bytesSeq
    }

    it("slices") {
      val buffer = IndexedBytes.create(ByteBuffer.wrap(bytesSeq))
      val slice = buffer.slice(2, 5)

      slice.iterator.toSeq should contain theSameElementsInOrderAs bytesSeq.slice(2, 5)
    }

    it("doesn't let slices interfere with one another") {
      val buffer = IndexedBytes.create(ByteBuffer.wrap(bytesSeq))
      val sliceIndices = Seq((1, 4), (2, 5), (3, 6), (2, 6))

      // Verify slices contents
      for (indices <- sliceIndices) {
        val slice = buffer.slice(indices._1, indices._2)
        slice.iterator.toSeq should contain theSameElementsInOrderAs (bytesSeq.view(indices._1,
                                                                                    indices._2
                                                                                   ))
      }

      // Verify them _again_, ensuring no slice ruined a previous slice.
      for (indices <- sliceIndices) {
        val slice = buffer.slice(indices._1, indices._2)
        slice.iterator.toSeq should contain theSameElementsInOrderAs (bytesSeq.view(indices._1,
                                                                                    indices._2
                                                                                   ))
      }
    }

    describe("exceptions") {
      it("should throw IndexOutOfBoundsException when applied out of bounds") {
        val buffer = IndexedBytes.create(ByteBuffer.wrap(bytesSeq))
        intercept[IndexOutOfBoundsException] {
          buffer(-1)
        }
        intercept[IndexOutOfBoundsException] {
          buffer(bytesSeq.length)
        }
      }

      it("iterators should throw NoSuchElementException after end") {
        val iterator = IndexedBytes.create(ByteBuffer.wrap(bytesSeq)).iterator
        while (iterator.hasNext) {
          iterator.next()
        }
        intercept[java.util.NoSuchElementException](iterator.next())
      }
    }
  }
}
