package io.treeverse.clients

import org.apache.spark.SerializableWritable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Exporter(apiClient: ApiClient, repoName: String, rootExportLocation: String) {
  def exportAllFromBranch(spark : SparkSession, branch: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    exportAllFromCommit(spark, commitID)
  }

  def exportAllFromCommit(spark : SparkSession, commitID: String): Unit = {
    val ns = apiClient.getStorageNamespace(repoName)
    val df = LakeFSContext.newDF(spark, repoName, commitID)
    df.createOrReplaceTempView("commit")

    val rel = rootExportLocation
    val actionsDF = spark.sql("SELECT 'copy' as action, * FROM commit")

    export(spark, ns, rel, actionsDF)
    spark.sparkContext.stop()
  }

  private def export(spark: SparkSession, ns: String, rel: String, actionsDF: DataFrame) =  {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val serializedConf = new SerializableWritable(hadoopConf)

    actionsDF.foreach { row =>
      RowHandler.handle(ns, rel, serializedConf, row)
    }
}
def exportFrom(spark : SparkSession, branch: String, prevCommitID: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    val ns = apiClient.getStorageNamespace(repoName)

    val newDF = LakeFSContext.newDF(spark, repoName, commitID)
    val prevDF = LakeFSContext.newDF(spark, repoName, prevCommitID)

    newDF.createOrReplaceTempView("new_commit")
    prevDF.createOrReplaceTempView("prev_commit")

    // pin Exporter field to avoid serialization
    val rel = rootExportLocation
    val actionsDF = spark.sql("SELECT 'copy' as action, new_commit.* FROM new_commit " +
      "LEFT JOIN prev_commit " +
      "ON (new_commit.etag = prev_commit.etag AND new_commit.key = prev_commit.key) " +
      "WHERE prev_commit.key is NULL " +
      "UNION " +
      "SELECT 'delete' as action, prev_commit.* FROM prev_commit " +
      "LEFT OUTER JOIN new_commit " +
      "ON (new_commit.key = prev_commit.key) " +
      "WHERE new_commit.key is NULL")

    export(spark, ns, rel, actionsDF)
    spark.sparkContext.stop()
  }
}

object RowHandler {
  def handle(ns: String, rel: String, serializedConf: SerializableWritable[org.apache.hadoop.conf.Configuration], row: Row) =  {
    val action = row(0)
    val key = row(1)
    val address = row(2).toString()
    val conf = serializedConf.value

    val srcPath = new org.apache.hadoop.fs.Path( if (address.contains("://")) address else ns + "/" + address)
    val dstPath = new org.apache.hadoop.fs.Path(rel + "/" + key)

    val dstFS = dstPath.getFileSystem(conf)

    action match {
      case "delete" => {
        dstFS.delete(dstPath,false) : Unit
      }

      case "copy" =>{
        org.apache.hadoop.fs.FileUtil.copy(

          srcPath.getFileSystem(conf),
          srcPath,
          dstFS,
          dstPath,
          false,
          conf
        ) : Unit
      }
    }
  }
}
