package io.treeverse.jpebble

import org.scalatest._
import matchers.should._
import funspec._

class ByteBufferIndexedBytesSpec extends AnyFunSpec with Matchers {
  val bytesSeq = Array[Byte](2, 3, 5, 7, 11, 13)

  /** Call fn(start, end) for every 0 <= start <= end <= s.size */
  def forPairs[T](s: Iterable[T], fn: (Int, Int) => Unit): Unit = {
    val size = s.size
      (0 to size).foreach(start =>
        (start to size).foreach(end =>
          fn(start, end)))
  }

  describe("ByteBufferIndexedBytes") {
    it("returns bytes in buffer through iterator") {
      val bytes = IndexedBytes.create(bytesSeq).iterator.toSeq

      bytes should contain theSameElementsInOrderAs bytesSeq
    }

    it("slices") {
      val buffer = IndexedBytes.create(bytesSeq)
      val slice = buffer.slice(2, 5)

      slice.iterator.toSeq should contain theSameElementsInOrderAs bytesSeq.slice(2, 5)
    }

    it("doesn't let slices interfere with one another") {
      val buffer = IndexedBytes.create(bytesSeq)
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

    describe("sliceView") {
      it("returns same elements as slice") {
        val buffer = IndexedBytes.create(bytesSeq)
        val bufferConcrete = buffer.asInstanceOf[BufferIndexedBytes]
        forPairs(bytesSeq, (start: Int, end: Int) =>
          bufferConcrete.sliceView(start, end)
            should contain theSameElementsInOrderAs (buffer.slice(start, end).iterator.toSeq))
      }
    }

    describe("iterator") {
      it("loops") {
        val buffer = IndexedBytes.create(bytesSeq)
        val iterator = buffer.iterator
        bytesSeq.foreach(b => {
          iterator.hasNext should be(true)
          iterator.next should equal(b)
        })
        iterator.hasNext should be(false)
      }

      it("loops on slices") {
        val buffer = IndexedBytes.create(bytesSeq)
        forPairs(bytesSeq, (start, end) => {
          val slice = buffer.slice(start, end)
          val iterator = slice.iterator
          (start to end-1).foreach(i => withClue(s"($start, $end)") {
            iterator.hasNext should be(true)
            iterator.next should equal(bytesSeq(i))
          })
          iterator.hasNext should be(false)
        })
      }

      it("takes") {
        val buffer = IndexedBytes.create(bytesSeq)
        val sliceIndices = Seq((1, 4), (2, 5), (3, 6), (2, 6))

        // Verify slices contents
        for (indices <- sliceIndices) {
          val takenX = bytesSeq.iterator.drop(indices._1).take(indices._2 - indices._1)
          takenX.toSeq should contain theSameElementsInOrderAs (bytesSeq.view(indices._1, indices._2 ))
          val taken = buffer.iterator.drop(indices._1).take(indices._2 - indices._1)
          taken.toSeq should contain theSameElementsInOrderAs (bytesSeq.view(indices._1, indices._2 ))
        }
      }
    }

    describe("exceptions") {
      it("should throw IndexOutOfBoundsException when applied out of bounds") {
        val buffer = IndexedBytes.create(bytesSeq)
        intercept[IndexOutOfBoundsException] {
          buffer(-1)
        }
        intercept[IndexOutOfBoundsException] {
          buffer(bytesSeq.length)
        }
      }

      it("iterators should throw NoSuchElementException after end") {
        val iterator = IndexedBytes.create(bytesSeq).iterator
        while (iterator.hasNext) {
          iterator.next()
        }
        intercept[java.util.NoSuchElementException](iterator.next())
      }
    }
  }
}
