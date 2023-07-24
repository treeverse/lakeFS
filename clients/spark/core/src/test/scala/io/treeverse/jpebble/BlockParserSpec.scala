package io.treeverse.jpebble

import org.scalatest._
import matchers.should._
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import funspec._

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.dimafeng.testcontainers.GenericContainer.FileSystemBind
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import org.testcontainers.containers.BindMode

import java.io.File
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils

import scala.io.Source

class BlockReadableSpec extends AnyFunSpec with Matchers {

  describe("instantiate BlockReadableFileChannel") {
    describe("with null file") {
      it("should fail with an IllegalArgumentException") {
        assertThrows[IllegalArgumentException] {
          new BlockReadableFile(null)
        }
      }
    }
  }
}

object BlockParserSpec {
  def str(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)
}

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
      assertThrows[BadFileFormatException] {
        BlockParser.readMagic(bytes.iterator)
      }
    }

    it("fails to read truncated magic") {
      val bytes = magicBytes.iterator.drop(3)
      assertThrows[BadFileFormatException] {
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
            decodedValue should be(value)
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
            decodedValue should be(value)
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
            decodedValue should be(value)
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
            decodedValue should be(value)
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
            decodedValue should be(value.toInt)
          }
        }
      })
    }
  }

  describe("read RocksDB SSTable") {
    // Load source data
    val hTxt = Source.fromInputStream(
      // Source.fromResource in Scala >= 2.12 but need to support 2.11.
      getClass.getClassLoader.getResourceAsStream("pebble-testdata/h.txt")
    )
    val histRe = " *(\\d+) *(\\w+) *$".r
    val expected = hTxt
      .getLines()
      .map((line) =>
        line match {
          case histRe(count, word) => (word, count.toInt)
          case _                   => throw new RuntimeException(s"Bad format h.txt line ${line}")
        }
      )
      .toSeq

    it("internal: load h.txt") {
      expected should not be empty
    }

    val testFiles = Seq(
      "h.sst",
      "h.block-bloom.no-compression.sst",
      "h.no-compression.sst",
      "h.no-compression.two_level_index.sst",
      "h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst",
      "h.table-bloom.no-compression.sst",
      "h.table-bloom.sst"
    )

    /** Lightweight fixture for running particular tests with an SSTable and
     *  recovering its handle after running.  See
     *  https://www.scalatest.org/scaladoc/1.8/org/scalatest/FlatSpec.html
     *  ("Providing different fixtures to different tests") for how this works.
     */
    def withSSTable(sstFilename: String, test: BlockReadable => Any) = {
      // Copy SSTable to a readable file
      val sstContents =
        this.getClass.getClassLoader.getResourceAsStream("pebble-testdata/" + sstFilename)
      val tempFile = File.createTempFile("test-block-parser.", ".sst")
      tempFile.deleteOnExit()
      val out = new java.io.FileOutputStream(tempFile)
      IOUtils.copy(sstContents, out)
      sstContents.close()
      out.close()

      val in = new BlockReadableFile(new java.io.RandomAccessFile(tempFile, "r"))
      try {
        test(in)
      } finally {
        in.close()
      }
    }

    testFiles.foreach((sstFilename) => {
      describe(sstFilename) {
        it("reads footer") {
          withSSTable(
            sstFilename,
            (in: BlockReadable) => {
              val bytes = in.iterate(in.length - BlockParser.footerLength, BlockParser.footerLength)
              val footer = BlockParser.readFooter(bytes)
              bytes.hasNext should be(false)
            }
          )
        }

        it("dumps properties") {
          withSSTable(
            sstFilename,
            (in: BlockReadable) => {
              val bytes = in.iterate(in.length - BlockParser.footerLength, BlockParser.footerLength)
              val footer = BlockParser.readFooter(bytes)

              val props = BlockParser.readProperties(in, footer)
            }
          )
        }

        it("reads everything") {
          withSSTable(
            sstFilename,
            (in: BlockReadable) => {
              val it = BlockParser.entryIterator(in)
              val actual = it
                .map((entry) =>
                  (BlockParserSpec.str(entry.key), BlockParserSpec.str(entry.value).toInt)
                )
                .toSeq

              actual should contain theSameElementsInOrderAs expected
            }
          )
        }
      }
    })
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
      ci.count should be(0)
      ci.next
      ci.count should be(1)
      ci.foreach((_) => Unit)
      ci.count should be(base.length)
    }
  }
}

class GolangContainerSpec extends AnyFunSpec with ForAllTestContainer {
  override val container: GenericContainer = GenericContainer(
    "golang:1.20.6-alpine",
    classpathResourceMapping = Seq(
      FileSystemBind("parser-test/sst_files_generator.go", "/local/sst_files_generator.go", BindMode.READ_WRITE),
      FileSystemBind("parser-test/go.mod", "/local/go.mod", BindMode.READ_WRITE),
      FileSystemBind("parser-test/go.sum", "/local/go.sum", BindMode.READ_WRITE)
    ),
    command = Seq("/bin/sh",
                  "-c",
                  "cd /local && CGO_ENABLED=0 go run sst_files_generator.go && echo \"done\""
                 ),
    waitStrategy = new LogMessageWaitStrategy().withRegEx(
      "done\\n"
    ) // TODO(Tals): use startupCheckStrategy instead of waitStrategy (https://github.com/treeverse/lakeFS/issues/2455)
  )

  describe("A block parser") {
    describe("with 2-level index sstable") {
      it("should parse successfully") {
        withGeneratedSstTestFiles("two.level.idx", verifyBlockParserOutput)
      }
    }

    describe("with multi-sized sstables") {
      val testFiles = Seq(
        "fuzz.contents.0.with.shared.prefix",
        "fuzz.contents.1",
        "fuzz.contents.2.with.shared.prefix",
        "fuzz.contents.3",
        "fuzz.contents.4.with.shared.prefix",
        "fuzz.contents.5"
      )

      testFiles.foreach(fileName =>
        describe(fileName) {
          it("should parse successfully") {
            withGeneratedSstTestFiles(fileName, verifyBlockParserOutput)
          }
        }
      )
    }

    describe("with random table user properties") {
      it("should parse successfully") {
        withGeneratedSstTestFiles("fuzz.table.properties", verifyBlockParserOutput)
      }
    }

    describe("with sstable with xxHash64 checksum") {
      it("should fail parsing") {
        assertThrows[BadFileFormatException] {
          withGeneratedSstTestFiles("checksum.type.xxHash64", verifyBlockParserOutput)
        }
      }
    }

    describe("with sstable with levelDB table format") {
      it("should fail parsing") {
        assertThrows[BadFileFormatException] {
          withGeneratedSstTestFiles("table.format.leveldb", verifyBlockParserOutput)
        }
      }
    }

    describe("with sstable with Zstd compression") {
      it("should fail parsing") {
        assertThrows[BadFileFormatException] {
          withGeneratedSstTestFiles("compression.type.zstd", verifyBlockParserOutput)
        }
      }
    }

    describe("with max size sstable supported by lakeFS") {
      it("should parse successfully") {
        withGeneratedSstTestFiles("max.size.lakefs.file", verifyBlockParserOutput)
      }
    }

    describe("with random restart interval") {
      it("should parse successfully") {
        withGeneratedSstTestFiles("fuzz.block.restart.interval", verifyBlockParserOutput)
      }
    }

    describe("with random block size") {
      it("should parse successfully") {
        withGeneratedSstTestFiles("fuzz.block.size", verifyBlockParserOutput)
      }
    }

    describe("with random blocksize threshold") {
      it("should parse successfully") {
        withGeneratedSstTestFiles("fuzz.block.size.threshold", verifyBlockParserOutput)
      }
    }

    describe("with empty file") {
      it("should fail parsing") {
        assertThrows[BadFileFormatException] {
          withGeneratedEmptyTestFile("empty.file", verifyBlockParserOutput)
        }
      }
    }

    describe("with sst file containing 0 records") {
      it("should parse successfully") {
        withGeneratedSstTestFiles("zero.records.sst", verifyBlockParserOutput)
      }
    }

    describe("with sstable with a corrupted magic mark") {
      it("should fail parsing") {
        assertThrows[BadFileFormatException] {
          withGeneratedSstTestFiles("bad.magic.mark", verifyBlockParserOutput)
        }
      }
    }
  }

  /** Copies data from a file inside the test container to a temporary file.
   *  @param baseFileName the file name without a suffix of the files to copy from within the container
   *  @param suffix of the file to copy from the container
   *  @return
   */
  def copyTestFile(baseFileName: String, suffix: String): File =
    container.copyFileFromContainer(
      "/local/" + baseFileName + suffix,
      in => {
        val tempOutFile = File.createTempFile("test-block-parser.", suffix)
        tempOutFile.deleteOnExit()
        val out = new java.io.FileOutputStream(tempOutFile)
        try {
          IOUtils.copy(in, out)
        } finally {
          out.close()
        }
        return tempOutFile
      }
    )

  /** Lightweight fixture for running particular tests with SSTables and JSON fules generated by a go application on
   *  test startup. The fixture uses sstables as the parser input, and the json as the source of the expected output of
   *  the parsing operation.
   */
  def withGeneratedSstTestFiles(
      baseFileName: String,
      test: (BlockReadable, Seq[(String, String)], Long) => Any
  ) = {
    val tmpSstFile = copyTestFile(baseFileName, ".sst")
    val tmpJsonFile = copyTestFile(baseFileName, ".json")

    val in = new BlockReadableFile(new java.io.RandomAccessFile(tmpSstFile, "r"))
    try {
      val jsonString = os.read(os.Path(tmpJsonFile.getAbsolutePath))
      val data = ujson.read(jsonString)
      val expected = data.arr.map(e => (e("Key").str, e("Value").str))
      test(in, expected, tmpSstFile.length())
    } finally {
      in.close()
    }
  }

  def withGeneratedEmptyTestFile(
      baseFileName: String,
      test: (BlockReadable, Seq[(String, String)], Long) => Any
  ) = {
    val tmpFile = copyTestFile(baseFileName, "")
    val in = new BlockReadableFile(new java.io.RandomAccessFile(tmpFile, "r"))
    try {
      test(in, null, tmpFile.length())
    } finally {
      in.close()
    }
  }

  def verifyBlockParserOutput(
      in: BlockReadable,
      expected: Seq[(String, String)],
      sstSize: Long
  ): Unit = {
    val it = BlockParser.entryIterator(in)
    val actual =
      it.map((entry) => (BlockParserSpec.str(entry.key), BlockParserSpec.str(entry.value))).toSeq

    actual should contain theSameElementsInOrderAs expected
  }
}
