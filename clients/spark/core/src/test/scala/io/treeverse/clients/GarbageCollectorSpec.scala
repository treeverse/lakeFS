package io.treeverse.clients

import scala.collection.JavaConverters._
import org.scalatest._
import matchers.should._
import funspec._

import io.treeverse.lakefs.catalog

import org.apache.commons.io.FileUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.{Dataset, SparkSession}

import org.json4s._
import org.json4s.native.JsonMethods

import java.nio.file.{Files, Path, Paths}

trait TempDirectory {
  def withTempDirectory(testMethod: (Path) => Any) {
    val tempDir = Files.createTempDirectory("test-gc")

    try {
      testMethod(tempDir)
    } finally {
      FileUtils.deleteDirectory(tempDir.toFile)
    }
  }
}

class ARangeGetter(
    val repo: String,
    val commitRanges: Map[String, Seq[String]],
    val ranges: Map[String, Seq[String]]
) extends RangeGetter
    with Serializable {
  private def verifyRepo(repo: String): Unit = {
    if (repo != this.repo) {
      throw new Exception(s"Expected repo ${this.repo} but got ${repo}")
    }
  }

  def getRangeIDs(commitID: String, repo: String): Iterator[String] = {
    verifyRepo(repo)
    commitRanges(commitID).iterator
  }

  def getRangeEntries(rangeID: String, repo: String): Iterator[catalog.Entry] = {
    verifyRepo(repo)
    ranges(rangeID)
      .map(a =>
        catalog.Entry.defaultInstance
          .withAddress(a)
          .withAddressType(
            if (a.contains("://")) catalog.Entry.AddressType.FULL
            else catalog.Entry.AddressType.RELATIVE
          )
      )
      .iterator
  }
}

class GarbageCollectorSpec extends AnyFunSpec with Matchers with SparkSessionSetup {
  describe("Spark") {
    it("should perform Gauss summation") {
      withSparkSession(spark => {
        val sc = spark.sparkContext
        val rdd = sc.parallelize(1 to 100)
        val total = rdd.reduce(_ + _)

        total should be(5050)
      })
    }
  }

  def compareDS[T](actual: Dataset[T], expected: Dataset[T]) = {
    val actualSet = actual.collect.toSet
    val expectedSet = expected.collect.toSet
    actualSet should be(expectedSet)
  }

  val getter = new ARangeGetter("repo",
                                null,
                                Map("aaa" -> Seq("a1", "a2", "s3://some-ns/a3"),
                                    "bbb" -> Seq("b1", "b2", "b3"),
                                    "ab12" -> Seq("a1", "a2", "b1", "b2"),
                                    "222" -> Seq("a2", "b2", "c2")
                                   )
                               )

  describe("GarbageCollector") {
    describe("minus") {
      it("removes elements in a simple case") {
        withSparkSession(spark => {
          import spark.implicits._
          val sc = spark.sparkContext
          val gc = new GarbageCollector(getter)

          val partitioner = new HashPartitioner(3)
          val threes = sc.parallelize(0 to 100 by 3).map(_.toString).toDS
          val sevens = sc.parallelize(0 to 100 by 7).map(_.toString).toDS
          val m = gc.minus(threes, sevens, partitioner).map(_.toInt)
          compareDS(m, sc.parallelize((0 to 100 by 3).filter(x => x % 7 != 0)).toDS)
        })
      }
    }

    describe("getAddressesToDelete") {
      val numRangePartitions = 3
      val numAddressPartitions = 7
      // (Primarily tests everything is Serializable!)
      it("should report nothing for nothing") {
        withSparkSession(spark => {
          import spark.implicits._
          val gc = new GarbageCollector(getter)

          val actualToDelete = gc.getAddressesToDelete(Seq[String]().toDS,
                                                       Seq[String]().toDS,
                                                       "repo",
                                                       "",
                                                       numRangePartitions,
                                                       numAddressPartitions
                                                      )
          val expectedToDelete = Seq[String]().toDS

          compareDS(actualToDelete, expectedToDelete)
        })
      }

      it("should report all elements in expired ranges when there is nothing to keep") {
        withSparkSession(spark => {
          import spark.implicits._
          val gc = new GarbageCollector(getter)

          val actualToDelete = gc.getAddressesToDelete(Seq("aaa", "222", "bbb").toDS,
                                                       Seq[String]().toDS,
                                                       "repo",
                                                       "s3://some-ns/",
                                                       numRangePartitions,
                                                       numAddressPartitions
                                                      )
          val expectedToDelete = Seq("a1", "a2", "a3", "b1", "b2", "b3", "c2").toDS

          compareDS(actualToDelete, expectedToDelete)
        })
      }

      it("should not remove kept elements") {
        withSparkSession(spark => {
          import spark.implicits._
          val gc = new GarbageCollector(getter)

          val actualToDelete = gc.getAddressesToDelete(Seq("aaa", "bbb").toDS,
                                                       Seq("222").toDS,
                                                       "repo",
                                                       "s3://some-other-ns/",
                                                       numRangePartitions,
                                                       numAddressPartitions
                                                      )
          val expectedToDelete = Seq("a1", "b1", "b3").toDS

          compareDS(actualToDelete, expectedToDelete)
        })
      }

      it("should not remove elements from kept ranges") {
        withSparkSession(spark => {
          import spark.implicits._
          val gc = new GarbageCollector(getter)

          val actualToDelete = gc.getAddressesToDelete(Seq("aaa", "bbb").toDS,
                                                       Seq("bbb").toDS,
                                                       "repo",
                                                       "s3://some-ns/",
                                                       numRangePartitions,
                                                       numAddressPartitions
                                                      )
          val expectedToDelete = Seq("a1", "a2", "a3").toDS

          compareDS(actualToDelete, expectedToDelete)
        })
      }

      it("should not remove elements even if asked to expire multiple times") {
        withSparkSession(spark => {
          import spark.implicits._
          val gc = new GarbageCollector(getter)

          val actualToDelete = gc.getAddressesToDelete(Seq("aaa", "bbb", "ab12").toDS,
                                                       Seq("bbb", "222").toDS,
                                                       "repo",
                                                       "s3://some-other-ns/",
                                                       numRangePartitions,
                                                       numAddressPartitions
                                                      )
          val expectedToDelete = Seq("a1").toDS

          compareDS(actualToDelete, expectedToDelete)
        })
      }
    }
  }
}

class GarbageCollectorJsonOutputSpec extends AnyFunSpec with Matchers with SparkSessionSetup with TempDirectory {
    describe("writeJsonSummary") {
      it("should write a summary") {
        withSparkSession(spark =>
          withTempDirectory(tempDir => {
            val sc = spark.sparkContext
            val configMapper = new ConfigMapper(sc.broadcast(Array[(String, String)]()))
            val dstRoot = tempDir.resolve("writeJsonSummary/")
            val numDeletedObjects = 2906
            val gcRules = "gobble gobble"
            val time = "I always will remember, 'Twas a year ago November"

            GarbageCollector.writeJsonSummaryForTesting(configMapper, dstRoot.toAbsolutePath.toString, numDeletedObjects, gcRules, time)

            val written = FileUtils.listFiles(dstRoot.toFile, null, true)
              .asScala
              .iterator
              .filter((f) => !f.toString.endsWith(".crc"))
              .toSeq
            written.size should be(1)
            val actualBytes = Files.readAllBytes(Paths.get(written(0).toString))
            // Explicitly verify that we received UTF-8 encoded data!
            val actual = JsonMethods.parse(new String(actualBytes, "UTF-8"))
            (actual \ "gc_rules") should be(JString(gcRules))
            (actual \ "num_deleted_objects") should be(JInt(numDeletedObjects))
            // TODO(ariels): Verify dt=${time} in path.
          }))
      }
    }
}

trait SparkSessionSetup {
  def withSparkSession(testMethod: (SparkSession) => Any) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark test")
      .set("spark.sql.shuffle.partitions", "17")
    val spark = new SparkSession.Builder().config(conf).getOrCreate
    testMethod(spark)
    // TODO(ariels): Can/should we "finally spark.stop()" just once, at the
    //     end of the entire suite?
  }
}
