package io.treeverse.clients
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.net.URL
import scala.util.Random

class Exporter(spark : SparkSession, apiClient: ApiClient, repoName: String, dstRoot: String) {
  def exportAllFromBranch(branch: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    exportAllFromCommit(commitID)
  }

  def exportAllFromCommit(commitID: String): Unit = {
    val ns = apiClient.getStorageNamespace(repoName)
    val df = LakeFSContext.newDF(spark, repoName, commitID)

    val tableName = Random.alphanumeric.dropWhile(_.isDigit).take(8).mkString("") + "_commit"
    df.createOrReplaceTempView(tableName)

    // pin Exporter field to avoid serialization
    val dst = dstRoot
    val actionsDF = spark.sql(s"SELECT 'copy' as action, * FROM ${tableName}")

    export(ns, dst, actionsDF, false)
    export(ns, dst, actionsDF, true)
  }


  private def export(ns: String, rel: String, actionsDF: DataFrame, successFiles: Boolean) =  {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val serializedConf = new SerializableWritable(hadoopConf)

    actionsDF.foreach { row =>
      Exporter.handleRow(ns, rel, serializedConf, row, successFiles)
    }
  }

  def exportFrom(branch: String, prevCommitID: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    val ns = apiClient.getStorageNamespace(repoName)

    val newDF = LakeFSContext.newDF(spark, repoName, commitID)
    val prevDF = LakeFSContext.newDF(spark, repoName, prevCommitID)

    val gen = Random.alphanumeric.dropWhile(_.isDigit)
    val newTableName =  gen.take(8).mkString("") +"_new_commit"
    val prevTableName =  gen.take(8).mkString("") +"_prev_commit"
    newDF.createOrReplaceTempView(newTableName)
    prevDF.createOrReplaceTempView(prevTableName)

    // pin Exporter field to avoid serialization
    val dst = dstRoot

    val actionsDF = spark.sql(s"""
    SELECT
      CASE WHEN nkey is null THEN 'delete' ELSE 'copy' END as action,
      CASE WHEN nkey is null THEN pkey ELSE nkey END as key,
      CASE WHEN naddress is null THEN paddress ELSE naddress END as address,
      CASE WHEN netag is null THEN petag ELSE netag END as etag
    FROM
    (SELECT n.key as nkey, n.address as naddress, n.etag as netag,
      p.key as pkey, p.address as paddress, p.etag as petag
      FROM ${newTableName} n
      FULL OUTER JOIN ${prevTableName} p
      ON n.key = p.key
      WHERE n.etag <> p.etag OR n.etag is null or p.etag is null)
    """)

    export(ns, dst, actionsDF, false)
    export(ns, dst, actionsDF, true)
  }
}

object Exporter {
  val sparkSuccessFileSuffix = "_SUCCESS"

  private def handleRow(ns: String, rootDst: String, serializedConf: SerializableWritable[Configuration], row: Row, successFiles: Boolean): Unit =  {
    val action = row(0)
    val key = row(1).toString()
    val address = row(2).toString()

    val isSuccessFile = key.endsWith(sparkSuccessFileSuffix)
    if (isSuccessFile != successFiles){
      return
    }

    val conf = serializedConf.value
    val srcPath = resolveURL(new URL(ns), address)
    val dstPath = resolveURL(new URL(rootDst), key)

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

  private def resolveURL(baseUrl :URL, extraPath: String): Path = {
    val uri = baseUrl.toURI
    val newPath: String = uri.getPath + '/' + extraPath

    new Path(uri.resolve(newPath))
  }
}
