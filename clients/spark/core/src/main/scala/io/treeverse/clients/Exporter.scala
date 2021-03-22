package io.treeverse.clients

import org.apache.spark.SerializableWritable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Exporter(spark : SparkSession, apiClient: ApiClient, repoName: String, dstRoot: String) {
  def exportAllFromBranch(branch: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    exportAllFromCommit(commitID)
  }

  def exportAllFromCommit(commitID: String): Unit = {
    val ns = apiClient.getStorageNamespace(repoName)
    val df = LakeFSContext.newDF(spark, repoName, commitID)
    df.createOrReplaceTempView("commit")

    // pin Exporter field to avoid serialization
    val dst = dstRoot
    val actionsDF = spark.sql("SELECT 'copy' as action, * FROM commit")

    export(ns, dst, actionsDF)
    spark.sparkContext.stop()
  }

  private def export(ns: String, rel: String, actionsDF: DataFrame) =  {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val serializedConf = new SerializableWritable(hadoopConf)

    actionsDF.foreach { row =>
      Exporter.handleRow(ns, rel, serializedConf, row)
    }
  }

  def exportFrom(branch: String, prevCommitID: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    val ns = apiClient.getStorageNamespace(repoName)

    val newDF = LakeFSContext.newDF(spark, repoName, commitID)
    val prevDF = LakeFSContext.newDF(spark, repoName, prevCommitID)

    newDF.createOrReplaceTempView("new_commit")
    prevDF.createOrReplaceTempView("prev_commit")

    // pin Exporter field to avoid serialization
    val dst = dstRoot

    val actionsDF = spark.sql("WITH raw_rows AS (SELECT n.key as nkey, n.address as naddress, n.etag as netag, " +
      "p.key as pkey, p.address as paddress, p.etag as petag " +
      "FROM new_commit n " +
      "FULL OUTER JOIN prev_commit p " +
      "ON n.key = p.key " +
      "WHERE n.etag <> p.etag OR n.etag is null or p.etag is null) " +
      "SELECT " +
      "CASE WHEN nkey is null THEN 'delete' ELSE 'copy' END as action, " +
      "CASE WHEN nkey is null THEN pkey ELSE nkey END as key, " +
      "CASE WHEN naddress is null THEN paddress ELSE naddress END as address, " +
      "CASE WHEN netag is null THEN petag ELSE netag END as etag ")

    export(ns, dst, actionsDF)
    spark.sparkContext.stop()
  }
}

object Exporter {
  private def handleRow(ns: String, rel: String, serializedConf: SerializableWritable[org.apache.hadoop.conf.Configuration], row: Row): Unit =  {
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
