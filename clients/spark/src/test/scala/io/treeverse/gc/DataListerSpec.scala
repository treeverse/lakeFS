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
            new ParallelDataLister().listData(configMapper, path).sort("base_address")
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
            new ParallelDataLister().listData(configMapper, path).sort("base_address")
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
          val df = new NaiveDataLister().listData(configMapper, path).sort("base_address")
          df.count should be(10)
          df.sort("base_address").head.getString(0) should be("object01")
          df.head.getString(0) should be("object01")
          df.sort(desc("base_address")).head.getString(0) should be("object10")
        })
      }
    }
  }
}
