package io.treeverse.parquet
import io.treeverse.clients.LakeFSContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class RepositoryConverter(
    val spark: SparkSession,
    val conf: Configuration,
    val repoName: String,
    val dstPath: Path
) {

  def convert(): Unit = {
    val fs = dstPath.getFileSystem(conf)
    var rangesToAdd = LakeFSContext.newDF(spark, repoName = repoName)
    try {
      val existingRanges = spark.read
        .option("spark.sql.files.ignoreMissingFiles", "true")
        .parquet(dstPath.toString)
        .select("range_id")
        .distinct
      rangesToAdd = rangesToAdd
        .as("r1")
        .join(existingRanges.as("r2"), Seq("range_id"), "left_anti")
        .select(col("r1.*"))
    } catch {
      case _: Throwable => {
        // TODO add debug log
      }
    }
    rangesToAdd.write
      .partitionBy("range_id")
      .mode("overwrite")
      .format("parquet")
      .option("partitionOverwriteMode", "dynamic")
      .save(dstPath.toString)
  }
}
