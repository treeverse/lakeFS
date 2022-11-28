package io.treeverse.gc

import io.treeverse.clients.SparkSessionSetup
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.Files
import java.time.format.DateTimeFormatter

class UncommittedGarbageCollectorSpec
    extends AnyFunSpec
    with SparkSessionSetup
    with should.Matchers
    with BeforeAndAfter
    with MockitoSugar {

  describe("UncommittedGarbageCollector") {
    describe(".writeReports") {
      var dir: java.nio.file.Path = null

      before {
        dir = Files.createTempDirectory("gc_plus_test")
      }

      after {
        FileUtils.deleteDirectory(dir.toFile)
      }

      it("should write a valid report") {
        withSparkSession(spark => {
          import spark.implicits._
          val runID = java.util.UUID.randomUUID.toString
          val startTime = java.time.Clock.systemUTC.instant()
          val firstSlice = "someSlice"
          val success = true
          val df = Seq("file1", "file2").toDF("address")

          UncommittedGarbageCollector.writeReports(dir.toString + "/",
                                                   runID,
                                                   firstSlice,
                                                   startTime,
                                                   success,
                                                   df
                                                  )

          val rootPath = java.nio.file.Paths.get("_lakefs", "retention", "gc", "uncommitted", runID)
          val summaryPath = dir.resolve(rootPath.resolve("summary.json"))
          val summary = ujson.read(os.read(os.Path(summaryPath)))

          summary("run_id").str should be(runID)
          summary("first_slice").str should be(firstSlice)
          summary("start_time").str should be(DateTimeFormatter.ISO_INSTANT.format(startTime))
          summary("success").bool should be(success)
          summary("num_deleted_objects").num should be(df.count())

          val deletedPath = dir.resolve(rootPath.resolve("deleted"))
          val deletedDF = spark.read.parquet(deletedPath.toString)
          deletedDF.count() should be(df.count())
          df.except(deletedDF.select(col("address"))).count() should be(0)
        })
      }
    }
  }
}
