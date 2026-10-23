package io.treeverse.clients

import io.treeverse.jpebble.{BadFileFormatException, BlockParser}
import io.treeverse.lakefs.catalog.Entry
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

class SSTableReaderSpec extends AnyFunSpec with Matchers {
  private def withBadTable(test: Path => Unit): Unit = {
    val path = Files.createTempFile("bad-range-", ".sst")
    Files.write(path, new Array[Byte](BlockParser.footerLength.toInt + 32))
    try test(path)
    finally Files.deleteIfExists(path)
  }

  describe("valid SSTables") {
    it("still reads range entries") {
      val url = getClass.getResource("/sstable/range.sst").toString
      val reader = SSTableReader.forRange(new Configuration(), url)
      try reader.newIterator().size should be > 0
      finally reader.close()
    }

    it("still reads metarange entries") {
      val url = getClass.getResource("/sstable/metarange.sst").toString
      val reader = SSTableReader.forMetaRange(new Configuration(), url)
      try reader.newIterator().size should be > 0
      finally reader.close()
    }
  }

  describe("SSTable corruption errors") {
    it("includes the original range URL when reading properties") {
      withBadTable { path =>
        val url = path.toUri.toString
        val reader = SSTableReader.forRange(new Configuration(), url)
        try {
          val error = intercept[BadFileFormatException](reader.getProperties)
          error.getMessage should include(url)
          error.getMessage should include("Bad magic")
          error.getCause shouldBe a[BadFileFormatException]
        } finally reader.close()
      }
    }

    it("includes the original metarange URL when creating an iterator") {
      withBadTable { path =>
        val url = path.toUri.toString
        val reader = SSTableReader.forMetaRange(new Configuration(), url)
        try {
          val error = intercept[BadFileFormatException](reader.newIterator())
          error.getMessage should include(url)
          error.getCause shouldBe a[BadFileFormatException]
        } finally reader.close()
      }
    }

    it("includes the local filename when no source URL is supplied") {
      withBadTable { path =>
        val reader = new SSTableReader(path.toString, Entry.messageCompanion, false)
        try {
          intercept[BadFileFormatException](reader.newIterator()).getMessage should include(
            path.toString
          )
        } finally reader.close()
      }
    }

    it("includes the source URL when a data block fails during lazy iteration") {
      val path = Files.createTempFile("bad-data-block-", ".sst")
      val input = getClass.getResourceAsStream("/sstable/range.sst")
      try Files.copy(input, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
      finally input.close()
      val bytes = Files.readAllBytes(path)
      bytes(0) = (bytes(0) ^ 1).toByte
      Files.write(path, bytes)
      val url = path.toUri.toString
      val reader = SSTableReader.forRange(new Configuration(), url)
      try {
        val iterator = reader.newIterator()
        val error = intercept[BadFileFormatException] {
          while (iterator.hasNext) iterator.next()
        }
        error.getMessage should include(url)
        error.getCause shouldBe a[BadFileFormatException]
      } finally {
        reader.close()
        Files.deleteIfExists(path)
      }
    }

    it("propagates the original filename from a Spark task to the driver") {
      withBadTable { path =>
        val url = path.toUri.toString
        val context = new SparkContext(
          new SparkConf()
            .setMaster("local[1]")
            .setAppName("SSTable error context")
            .set("spark.ui.enabled", "false")
        )
        try {
          val error = intercept[SparkException] {
            context
              .parallelize(Seq(url), 1)
              .map { source =>
                val reader = SSTableReader.forRange(new Configuration(), source)
                try reader.newIterator().hasNext
                finally reader.close()
              }
              .collect()
          }
          error.getMessage should include(url)
          error.getMessage should include("Bad magic")
        } finally context.stop()
      }
    }
  }
}
