package io.treeverse.jpebble

import org.scalatest._
import matchers.should._
import funspec._

import java.io.File
import org.apache.commons.io.IOUtils

class BlockParserSpec extends AnyFunSpec with Matchers {
  val magicBytes = BlockParser.footerMagic

  describe("readMagic") {
    it("can read magic exactly") {
      val bytes = magicBytes.iterator
      BlockParser.readMagic(bytes)
      BlockParser.readEnd(bytes)
    }

    it("can read magic with a suffix") {
      val bytes = magicBytes :+ 123.toByte
      BlockParser.readMagic(magicBytes.iterator)
    }

    it("fails to read changed magic") {
      val bytes = magicBytes.toArray
      bytes(2) = (bytes(2) + 3).toByte
      assertThrows[IllegalArgumentException]{
        BlockParser.readMagic(bytes.iterator)
      }
    }

    it("fails to read truncated magic") {
      val bytes = magicBytes.iterator.drop(3)
      assertThrows[IllegalArgumentException]{
        BlockParser.readMagic(bytes)
      }
    }
  }

  describe("readUnsignedVarLong") {
    describe("handles sample case in doc") {
      Seq(
        ("parses 300", Seq(0xac, 0x02), 300)
      ).foreach({
        case ((name, b, value)) => {
          it(name) {
            val decodedValue = BlockParser.readUnsignedVarLong(b.map(_.toByte).iterator)
            decodedValue should be (value)
          }
        }
      })
    }

    describe("handles test cases from Golang encoding/binary test code") {
      // Generated from test code with https://play.golang.org/p/OWVVVkjmwQJ
      Seq(
        (Seq(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01), -9223372036854775808L),
        (Seq(0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01), -9223372036854775807L),
        (Seq(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01), -1),
        (Seq(0x00), 0),
        (Seq(0x01), 1),
        (Seq(0x02), 2),
        (Seq(0x0a), 10),
        (Seq(0x14), 20),
        (Seq(0x3f), 63),
        (Seq(0x40), 64),
        (Seq(0x41), 65),
        (Seq(0x7f), 127),
        (Seq(0x80, 0x01), 128),
        (Seq(0x81, 0x01), 129),
        (Seq(0xff, 0x01), 255),
        (Seq(0x80, 0x02), 256),
        (Seq(0x81, 0x02), 257),
        (Seq(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f), 9223372036854775807L)
      ).foreach({
        case ((b, value)) => {
          it(s"parses ${value}") {
            val decodedValue = BlockParser.readUnsignedVarLong(b.map(_.toByte).iterator)
            decodedValue should be (value)
          }
        }
      })
    }
  }

  describe("readSignedVarLong") {
    describe("handles sample cases in docs") {
      Seq(
        ("parses zero", Seq(0x00), 0),
        ("parses -1", Seq(0x01), -1),
        ("parses 1", Seq(0x02), 1),
        ("parses -2", Seq(0x03), -2),
        ("parses 2", Seq(0x04), 2)
      ).foreach({
        case ((name, b, value)) => {
          it(name) {
            val decodedValue = BlockParser.readSignedVarLong(b.map(_.toByte).iterator)
            decodedValue should be (value)
          }
        }
      })
    }

    describe("handles test cases from Golang encoding/binary test code") {
      // Generated from test code with https://play.golang.org/p/O3bYzG8zUUX
      Seq(
        (Seq(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01), -9223372036854775808L),
        (Seq(0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01), -9223372036854775807L),
        (Seq(0x01), -1),
        (Seq(0x00), 0),
        (Seq(0x02), 1),
        (Seq(0x04), 2),
        (Seq(0x14), 10),
        (Seq(0x28), 20),
        (Seq(0x7e), 63),
        (Seq(0x80, 0x01), 64),
        (Seq(0x82, 0x01), 65),
        (Seq(0xfe, 0x01), 127),
        (Seq(0x80, 0x02), 128),
        (Seq(0x82, 0x02), 129),
        (Seq(0xfe, 0x03), 255),
        (Seq(0x80, 0x04), 256),
        (Seq(0x82, 0x04), 257),
        (Seq(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01), 9223372036854775807L)
      ).foreach({
        case ((bytes, value)) => {
          it(s"parses ${value}") {
            val decodedValue = BlockParser.readSignedVarLong(bytes.map(_.toByte).iterator)
            decodedValue should be (value)
          }
        }
      })
    }
  }

  describe("readInt32") {
    describe("reads little-endian int32s") {
      Seq[(Seq[Int], Long)](
        (Seq(0xff, 0xff, 0xff, 0xff), -1),
        (Seq(0xff, 0xff, 0xff, 0x7f), 0x7fffffff),
        (Seq(0, 0, 0, 0x80), -0x80000000L),
        (Seq(0, 1, 0, 0), 256),
        (Seq(1, 2, 3, 4), 0x04030201),
        (Seq(1, 1, 0, 0), 257),
        (Seq(0x66, 0x77, 0x88, 0x99), 0x99887766L)
      ).foreach({
        case ((bytes, value)) => {
          it(s"parses ${value.toInt}") {
            val decodedValue = BlockParser.readInt32(bytes.map(_.toByte).iterator)
            decodedValue should be (value.toInt)
          }
        }
      })
    }
  }

  describe("read RocksDB SSTable") {
    // Copy SSTable to a readable file
    val sstContents = this.getClass.getClassLoader.getResourceAsStream("sstable/ok1.sst")
    val tempFile = File.createTempFile("test-block-parser.", ".sst")
    tempFile.deleteOnExit()
    val out = new java.io.FileOutputStream(tempFile)
    IOUtils.copy(sstContents, out)
    sstContents.close()
    out.close()

    val in = new BlockReadableFileChannel(new java.io.FileInputStream(tempFile).getChannel)

    // TODO(ariels): Close after test.

    it("reads footer") {
      val bytes = in.iterate(tempFile.length - BlockParser.footerLength, BlockParser.footerLength)
      val footer = BlockParser.readFooter(bytes)
      bytes.hasNext should be (false)
    }

    it("reads metaindex") {
      // Read handles from footer
      val footerBytes = in.iterate(tempFile.length - BlockParser.footerLength, BlockParser.footerLength)
      val footer = BlockParser.readFooter(footerBytes)

      val bh = footer.metaIndex
      Console.out.println(f"[DEBUG]    metaindex ${bh.offset}%08x (${bh.offset}) -> ${bh.size}%08x (${bh.size}%d)")

      val bytes = in.readBlock(
        footer.metaIndex.offset,
        footer.metaIndex.size + BlockParser.blockTrailerLen)
      val block = BlockParser.startBlockParse(bytes)
      val blockIt = BlockParser.parseDataBlock(block)
      for (entry <- blockIt) {
        Console.out.println(s"[DEBUG]      ${entry}")
      }
    }
  }
}

class CountedIteratorSpec extends AnyFunSpec with Matchers {
  val base = Seq[Byte](2, 3, 5, 7, 11)

  describe("CountedIterator") {
    it("returns original elements") {
      val actual = new CountedIterator(base.iterator).toSeq
      actual should contain theSameElementsInOrderAs base
    }

    it("counts elements") {
      val ci = new CountedIterator(base.iterator)
      ci.count should be (0)
      ci.next
      ci.count should be (1)
      ci.foreach((_) => Unit)
      ci.count should be (base.length)
    }
  }
}
