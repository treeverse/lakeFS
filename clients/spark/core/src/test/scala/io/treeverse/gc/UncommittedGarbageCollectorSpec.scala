package io.treeverse.gc

import io.treeverse.clients.SparkSessionSetup
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfter
import org.scalatest.ParallelTestExecution
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import org.scalatestplus.mockito.MockitoSugar

import java.io.File
import java.nio.file.{Files, Path}
import java.time.format.DateTimeFormatter
import java.util.Date

class UncommittedGarbageCollectorSpec
    extends AnyFunSpec
    with SparkSessionSetup
    with should.Matchers
    with BeforeAndAfter
    with MockitoSugar
    with ParallelTestExecution {

  describe("UncommittedGarbageCollector") {
    var dir: java.nio.file.Path = null
    val repo = "gc_plus_test"

    before {
      dir = Files.createTempDirectory(repo)
    }

    after {
      FileUtils.deleteDirectory(dir.toFile)
    }

    def createSliceData(p: Path): List[String] = {
      var ds = List[String]()
      if (!Files.exists(p)) {
        p.toFile.mkdir()
      }
      for (j <- 1 to 10) {
        val objectID = f"object$j%02d"
        val file = new File(p.toFile, objectID)
        file.createNewFile()
        ds = ds :+ file.toString.substring(dir.toString.length + 1)

      }
      ds
    }

    def createData(prefix: String): List[String] = {
      var ds = List[String]()
      val p = dir.resolve(prefix)
      if (!Files.exists(p)) {
        p.toFile.mkdir()
      }

      for (i <- 1 to 10) {
        val sliceID = f"slice$i%02d"
        val slice = new File(p.toFile, sliceID)
        slice.mkdir()
        ds = ds ::: createSliceData(slice.toPath)
      }
      ds
    }

    def assertDSEqual[T](actual: Dataset[T], expected: Dataset[T]) = {
      val actualSet = actual.collect.toSet
      val expectedSet = expected.collect.toSet
      actualSet should be(expectedSet)
    }

    describe(".listObjects") {
      it("should return nothing") {
        withSparkSession(_ => {
          var dataDF =
            UncommittedGarbageCollector.listObjects(dir.toString, new Date())
          dataDF.count() should be(0)

          val dataDir = new File(dir.toFile, "data")
          dataDir.mkdir()

          dataDF = UncommittedGarbageCollector.listObjects(dir.toString, new Date())
          dataDF.count() should be(0)
          UncommittedGarbageCollector.getFirstSlice(dataDF, repo) should be("")
        })
      }

      it("should return elements on root") {
        withSparkSession(spark => {
          import spark.implicits._
          val data = createSliceData(dir.resolve(""))

          val dataDF = UncommittedGarbageCollector.listObjects(dir.toString,
                                                               DateUtils.addHours(new Date(), +1)
                                                              )
          dataDF.count() should be(10)
          val actual = dataDF.select("address").map(_.getString(0)).collect.toSeq.toDS()
          val expected = data.toDS()
          assertDSEqual(actual, expected)
          UncommittedGarbageCollector.getFirstSlice(dataDF, repo) should be("")
        })
      }

      it("should return elements on data") {
        withSparkSession(spark => {
          import spark.implicits._
          val data = createData("data")

          val dataDF =
            UncommittedGarbageCollector.listObjects(dir.toString,
                                                    DateUtils.addHours(new Date(), +1)
                                                   )
          dataDF.count() should be(100)
          val actual = dataDF.select("address").map(_.getString(0)).collect.toSeq.toDS()
          val expected = data.toDS()
          assertDSEqual(actual, expected)
        })
      }

      it("should return elements on all paths") {
        withSparkSession(spark => {
          import spark.implicits._
          val data = createSliceData(dir.resolve("")) ::: createData("data")

          val dataDF =
            UncommittedGarbageCollector.listObjects(dir.toString,
                                                    DateUtils.addHours(new Date(), +1)
                                                   )
          dataDF.count() should be(110)
          val actual = dataDF.select("address").map(_.getString(0)).collect.toSeq.toDS()
          val expected = data.toDS()
          assertDSEqual(actual, expected)
        })
      }

      it("should not list objects with timestamp > before") {
        withSparkSession(_ => {
          createSliceData(dir.resolve(""))
          createData("data")

          val dataDF =
            UncommittedGarbageCollector.listObjects(dir.toString,
                                                    DateUtils.addHours(new Date(), -1)
                                                   )
          dataDF.count() should be(0)
          UncommittedGarbageCollector.getFirstSlice(dataDF, repo) should be("")
        })
      }

      it("should return empty first slice with only staged objects") {
        withSparkSession(_ => {
          val dataDir = new File(dir.toFile, "data")
          dataDir.mkdir()
          val legacyPath = repo + "_legacy_physical:address_path"
          new File(dataDir, legacyPath).createNewFile()

          val dataDF =
            UncommittedGarbageCollector.listObjects(dir.toString,
                                                    DateUtils.addHours(new Date(), +1)
                                                   )
          dataDF.count() should be(1)
          UncommittedGarbageCollector.getFirstSlice(dataDF, repo) should be("")
        })
      }

      it("should return empty first slice with only old repository data") {
        withSparkSession(_ => {
          val dataDir = new File(dir.toFile, "")
          dataDir.mkdir()
          val filename = "some_file"
          new File(dataDir, filename).createNewFile()

          val dataDF =
            UncommittedGarbageCollector.listObjects(dir.toString,
                                                    DateUtils.addHours(new Date(), +1)
                                                   )
          dataDF.count() should be(1)
          UncommittedGarbageCollector.getFirstSlice(dataDF, repo) should be("")
        })
      }

      it("should return correct slice") {
        withSparkSession(_ => {
          val dataDir = new File(dir.toFile, "data")
          dataDir.mkdir()
          val legacyPath = repo + "_legacy_physical:address_path"
          val regularSlice = "xxx"
          val regularSlice2 = "yyy"
          val filename = "some_file"
          var slice = new File(dataDir, legacyPath)
          slice.mkdir()
          new File(slice, filename).createNewFile()
          slice = new File(dataDir, regularSlice)
          slice.mkdir()
          new File(slice, filename).createNewFile()
          slice = new File(dataDir, regularSlice2)
          slice.mkdir()
          new File(slice, filename).createNewFile()

          val dataDF = UncommittedGarbageCollector
            .listObjects(dir.toString, DateUtils.addHours(new Date(), +1))
            .sort("address")
          dataDF.count() should be(3)
          dataDF.select("address").head.getString(0) should be(
            s"data/$legacyPath/$filename"
          )
          UncommittedGarbageCollector.getFirstSlice(dataDF, repo) should be(regularSlice)
        })
      }
    }

    describe(".writeReports") {
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
    describe(".readMarkedAddresses") {
      it("should raise exception on failed run") {
        withSparkSession(_ => {
          val runID = "failed-run"
          val runPath =
            dir.resolve(java.nio.file.Paths.get("_lakefs", "retention", "gc", "uncommitted", runID))
          runPath.toFile.mkdirs()
          UncommittedGarbageCollector.writeJsonSummary(runPath.toString,
                                                       runID,
                                                       "",
                                                       java.time.Clock.systemUTC.instant(),
                                                       java.time.Clock.systemUTC.instant(),
                                                       false,
                                                       0
                                                      )
          try {
            UncommittedGarbageCollector.readMarkedAddresses(dir.toString + "/",
                                                            runID
                                                           ) // Should throw an exception
            // Fail test if no exception was thrown
            throw new Exception("test failed")
          } catch {
            // Other types of exceptions will not be caught and test will fail
            case e: FailedRunException =>
              e.getMessage.contains(s"Provided mark ($runID) is of a failed run") should be(true)
          }
        })
      }
      it("should not fail on run without deleted path") {
        withSparkSession(_ => {
          val runID = "no-deleted"
          val runPath =
            dir.resolve(java.nio.file.Paths.get("_lakefs", "retention", "gc", "uncommitted", runID))
          runPath.toFile.mkdirs()
          UncommittedGarbageCollector.writeJsonSummary(runPath.toString,
                                                       runID,
                                                       "",
                                                       java.time.Clock.systemUTC.instant(),
                                                       java.time.Clock.systemUTC.instant(),
                                                       true,
                                                       0
                                                      )

          val df = UncommittedGarbageCollector.readMarkedAddresses(dir.toString + "/", runID)
          df.isEmpty should be(true)
        })
      }
      it("should raise exception on missing run") {
        withSparkSession(_ => {
          val runID = "not-exist"
          try {
            UncommittedGarbageCollector.readMarkedAddresses(dir.toString + "/",
                                                            runID
                                                           ) // Should throw an exception
            // Fail test if no exception was thrown
            throw new Exception("test failed")
          } catch {
            // Other types of exceptions will not be caught and test will fail
            case e: FailedRunException =>
              e.getMessage.contains(s"Mark ID ($runID) does not exist") should be(true)
          }
        })
      }
    }
    describe(".validateRunModeConfigs") {
      val markID = "markID"

      it("should succeed mark & sweep") {
        UncommittedGarbageCollector.validateRunModeConfigs(true, true, "")
      }
      it("should succeed when sweep with mark ID") {
        UncommittedGarbageCollector.validateRunModeConfigs(false, true, markID)
      }
      it("should fail when no options provided") {
        try {
          UncommittedGarbageCollector.validateRunModeConfigs(false,
                                                             false,
                                                             markID
                                                            ) // Should throw an exception
          // Fail test if no exception was thrown
          throw new Exception("test failed")
        } catch {
          // Other types of exceptions will not be caught and test will fail
          case e: ParameterValidationException =>
            e.getMessage.contains(
              "Nothing to do, must specify at least one of mark, sweep"
            ) should be(true)
        }
      }
      it("should fail when mark with mark ID") {
        for (sweepVal <- Seq(true, false)) {
          try {
            UncommittedGarbageCollector.validateRunModeConfigs(true,
                                                               sweepVal,
                                                               markID
                                                              ) // Should throw an exception
            // Fail test if no exception was thrown
            throw new Exception("test failed")
          } catch {
            // Other types of exceptions will not be caught and test will fail
            case e: ParameterValidationException =>
              e.getMessage.contains("Can't provide mark ID for mark mode") should be(true)
          }
        }
      }
      it("should fail when sweep with no mark ID") {
        try {
          UncommittedGarbageCollector.validateRunModeConfigs(false,
                                                             true,
                                                             ""
                                                            ) // Should throw an exception
          // Fail test if no exception was thrown
          throw new Exception("test failed")
        } catch {
          // Other types of exceptions will not be caught and test will fail
          case e: ParameterValidationException =>
            e.getMessage.contains("Please provide a mark ID") should be(true)
        }
      }
    }
  }
}
