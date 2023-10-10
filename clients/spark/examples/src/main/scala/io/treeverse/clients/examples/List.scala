package io.treeverse.clients.examples

import io.treeverse.clients.LakeFSContext
import org.apache.spark.sql.SparkSession
import io.treeverse.clients.LakeFSJobParams

object List extends App {
  private def dirs(path: String): Seq[String] =
    path
      .split("/")
      .dropRight(1)
      .scanLeft("")((a, b: String) => (if (a.isEmpty) "" else a + "/") + b)

  override def main(args: Array[String]) {
    if (args.length != 3) {
      Console.err.println("Usage: ... <repo_name> <commit_id> s3://path/to/output/du")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("I can list").getOrCreate()

    val sc = spark.sparkContext
    val repo = args(0)
    val ref = args(1)
    val outputPath = args(2)
    val files = LakeFSContext.newRDD(sc, LakeFSJobParams.forCommit(repo, ref, "list_example"))

    val size = files
      .flatMap({ case (key, entry) => dirs(new String(key)).map(d => (d, entry.message.size)) })
      .reduceByKey(_ + _)

    size.saveAsTextFile(outputPath)

    sc.stop()
  }
}
