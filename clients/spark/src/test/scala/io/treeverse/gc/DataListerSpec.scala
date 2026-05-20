package io.treeverse.gc

import io.treeverse.clients.SparkSessionSetup
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.scalatest._

import java.io.File
import java.nio.file.Files
import org.apache.spark.sql.functions._

import funspec._
import matchers._
import io.treeverse.clients.ConfigMapper
import io.treeverse.clients.HadoopUtils

class ParallelDataListerSpec
    extends AnyFunSpec
    with SparkSessionSetup
    with should.Matchers
    with BeforeAndAfter {
  describe("ParallelDataListerSpec") {
    describe(".listData") {
      var dir: java.nio.file.Path = null

      before {
        dir = Files.createTempDirectory("gc_plus_test")
      }

      after {
        FileUtils.deleteDirectory(dir.toFile)
      }

      it("should list the correct slices and files") {
        val dataDir = new File(dir.toFile, "data")
        dataDir.mkdir()
        withSparkSession(spark => {
          for (i <- 1 to 10) {
            val sliceID = f"slice$i%02d"
            val slice = new File(dataDir, sliceID)
            slice.mkdir()
            for (j <- 1 to 10) {
              val objectID = f"object$j%02d"
              new File(slice, objectID).createNewFile()
            }
          }

          val path = new Path(dataDir.toURI)
          val configMapper = new ConfigMapper(
            spark.sparkContext.broadcast(
              HadoopUtils.getHadoopConfigurationValues(spark.sparkContext.hadoopConfiguration)
            )
          )
          val df =
            new ParallelDataLister().listData(configMapper, path, 3).sort("base_address")
          df.count should be(100)
          val slices =
            df.select(substring(col("base_address"), 0, 7).as("slice_id"))
              .select("slice_id")
              .distinct
          slices.count should be(10)
          slices.sort("slice_id").head.getString(0) should be("slice01")
          df.head.getString(0) should be("slice01/object01")
          df.sort(desc("base_address")).head.getString(0) should be("slice10/object10")
        })
      }

      // Note: these tests use the local file:// filesystem, which does not exhibit the URI-scheme mismatch
      // (s3:// vs s3a://) that motivated makeQualified in listPath. The require() assertion in listPath provides a
      // runtime guard against that mismatch in production, and the S3A coverage is provided by integration tests.
      it("should list sharded-format paths recursively") {
        val dataDir = new File(dir.toFile, "data")
        dataDir.mkdir()
        withSparkSession(spark => {
          // Create data/<shard>/<sub-shard>/<partition>/<xid> structure
          val shardDir = new File(dataDir, "!ab")
          shardDir.mkdir()
          val subShardDir = new File(shardDir, "c")
          subShardDir.mkdir()
          val partitionDir = new File(subShardDir, "bpel26ce4m36oefv1600")
          partitionDir.mkdir()
          for (j <- 1 to 5) {
            new File(partitionDir, f"xid$j%020d").createNewFile()
          }

          val path = new Path(dataDir.toURI)
          val configMapper = new ConfigMapper(
            spark.sparkContext.broadcast(
              HadoopUtils.getHadoopConfigurationValues(spark.sparkContext.hadoopConfiguration)
            )
          )
          val df = new ParallelDataLister().listData(configMapper, path, 3).sort("base_address")
          df.count should be(5)
          df.head.getString(0) should startWith("!ab/")
          df.head.getString(0).split("/").length should be(4)
          df.head.getString(0) should be("!ab/c/bpel26ce4m36oefv1600/xid00000000000000000001")
        })
      }

      it("should handle mixed sharded and legacy paths") {
        val dataDir = new File(dir.toFile, "data")
        dataDir.mkdir()
        withSparkSession(spark => {
          // Sharded: data/!ab/c/partition/xid
          val shardDir = new File(dataDir, "!ab")
          shardDir.mkdir()
          val subShardDir = new File(shardDir, "c")
          subShardDir.mkdir()
          val shardPartition = new File(subShardDir, "bpel26ce4m36oefv1600")
          shardPartition.mkdir()
          new File(shardPartition, "xid_sharded_file").createNewFile()

          // Legacy: data/<partition>/<xid>
          val legacyPartition = new File(dataDir, "legacy_partition_xid_str00")
          legacyPartition.mkdir()
          new File(legacyPartition, "xid_legacy_file").createNewFile()

          val path = new Path(dataDir.toURI)
          val configMapper = new ConfigMapper(
            spark.sparkContext.broadcast(
              HadoopUtils.getHadoopConfigurationValues(spark.sparkContext.hadoopConfiguration)
            )
          )
          val df = new ParallelDataLister().listData(configMapper, path, 3).sort("base_address")
          df.count should be(2)
          val addresses = df.collect().map(_.getString(0))
          // sharded entries should precede legacy partitions
          addresses(0) should be("!ab/c/bpel26ce4m36oefv1600/xid_sharded_file")
          addresses(1) should be("legacy_partition_xid_str00/xid_legacy_file")
        })
      }

      it("should return empty when path does not exist") {
        withSparkSession(spark => {
          val nonExistentDir = new File(dir.toFile, "nonexistent")
          val path = new Path(nonExistentDir.toURI)
          val configMapper = new ConfigMapper(
            spark.sparkContext.broadcast(
              HadoopUtils.getHadoopConfigurationValues(spark.sparkContext.hadoopConfiguration)
            )
          )
          val df = new ParallelDataLister().listData(configMapper, path, 3)
          df.count() should be(0)
        })
      }

      it("should list files placed directly in path") {
        val dataDir = new File(dir.toFile, "data")
        dataDir.mkdir()
        withSparkSession(spark => {
          val filename = "staged_object"
          new File(dataDir, filename).createNewFile()
          val path = new Path(dataDir.toURI)
          val configMapper = new ConfigMapper(
            spark.sparkContext.broadcast(
              HadoopUtils.getHadoopConfigurationValues(spark.sparkContext.hadoopConfiguration)
            )
          )
          val df = new ParallelDataLister().listData(configMapper, path, 3)
          df.count() should be(1)
          df.head.getString(0) should be(s"$filename/$filename")
        })
      }

      it("should be able to list path with ':' in it") {
        val dataDir = new File(dir.toFile, "data")
        dataDir.mkdir()
        withSparkSession(spark => {
          val sliceID = "legacy_physical:address_path"
          val filename = "some_file"
          val slice = new File(dataDir, sliceID)
          slice.mkdir()
          new File(slice, filename).createNewFile()

          val path = new Path(dataDir.toURI)
          val configMapper = new ConfigMapper(
            spark.sparkContext.broadcast(
              HadoopUtils.getHadoopConfigurationValues(spark.sparkContext.hadoopConfiguration)
            )
          )
          val df =
            new ParallelDataLister().listData(configMapper, path, 3).sort("base_address")
          df.count() should be(1)
          df.head.getString(0) should be(s"$sliceID/$filename")
        })
      }
    }
  }
}

class NaiveDataListerSpec
    extends AnyFunSpec
    with SparkSessionSetup
    with should.Matchers
    with BeforeAndAfter {
  describe("NaiveDataListerSpec") {
    describe(".listData") {
      var dir: java.nio.file.Path = null

      before {
        dir = Files.createTempDirectory("gc_plus_test")
      }

      after {
        FileUtils.deleteDirectory(dir.toFile)
      }

      it("should list the correct files and ignore slice") {
        withSparkSession(spark => {
          for (i <- 1 to 10) {
            val objectID = f"object$i%02d"
            new File(dir.toFile, objectID).createNewFile()
          }

          val slice = new File(dir.toFile, "slice")
          slice.mkdir()
          for (j <- 1 to 10) {
            val objectID = f"object$j%02d"
            new File(slice, objectID).createNewFile()
          }

          val path = new Path(dir.toFile.toURI)
          val configMapper = new ConfigMapper(
            spark.sparkContext.broadcast(
              HadoopUtils.getHadoopConfigurationValues(spark.sparkContext.hadoopConfiguration)
            )
          )
          val df = new NaiveDataLister().listData(configMapper, path, 3).sort("base_address")
          df.count should be(10)
          df.sort("base_address").head.getString(0) should be("object01")
          df.head.getString(0) should be("object01")
          df.sort(desc("base_address")).head.getString(0) should be("object10")
        })
      }
    }
  }
}
