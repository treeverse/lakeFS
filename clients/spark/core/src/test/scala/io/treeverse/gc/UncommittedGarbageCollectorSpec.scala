package io.treeverse.gc

import io.treeverse.clients.SparkSessionSetup
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructType}
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

  describe(".UncommittedGC") {
    var dir: java.nio.file.Path = null

    before {
      dir = Files.createTempDirectory("gc_plus_test")
    }

    after {
      FileUtils.deleteDirectory(dir.toFile)
    }

    it("should write a valid report") {
      withSparkSession(spark => {
        val runID = java.util.UUID.randomUUID.toString
        val startTime = DateTimeFormatter.ISO_INSTANT.format(java.time.Clock.systemUTC.instant())
        val lastSlice = "someSlice"
        val markID = "someMarkID"
        val success = true
        val data = Seq(Row("file1"), Row("file2"))
        val rdd = spark.sparkContext.parallelize(data)
        val schema = new StructType().add("address", StringType)
        val df = spark.createDataFrame(rdd, schema)

        UncommittedGarbageCollector.writeReports(dir.toString + "/",
                                                 runID,
                                                 markID,
                                                 lastSlice,
                                                 startTime,
                                                 success,
                                                 df
                                                )

        val rootPath = java.nio.file.Paths.get("_lakefs", "retention", "gc", "uncommitted", runID)
        val summaryPath = dir.resolve(rootPath.resolve("summary.json"))
        val summary = ujson.read(os.read(os.Path(summaryPath)))

        summary("run_id").str should be(runID)
        summary("last_slice").str should be(lastSlice)
        summary("start_time").str should be(startTime)
        summary("success").bool should be(success)
        summary("num_deleted_objects").num should be(df.count())

        val deletedPath = dir.resolve(rootPath.resolve("deleted.parquet"))
        val deletedDF = spark.read.parquet(deletedPath.toString)
        deletedDF.count() should be(df.count())
        df.except(deletedDF.select(col("address"))).count() should be(0)
      })
    }
  }
}
