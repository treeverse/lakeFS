package io.treeverse.clients
import io.treeverse.clients.Exporter.resolveURL
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{FileNotFoundException, IOException}
import java.net.URL
import scala.util.Random


class Exporter(spark : SparkSession, apiClient: ApiClient, filter: KeyFilter, repoName: String, dstRoot: String) {

  def this(spark : SparkSession, apiClient: ApiClient, repoName: String, dstRoot: String) {
    this(spark, apiClient, new SparkFilter(), repoName, dstRoot)
  }

  def exportAllFromBranch(branch: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    exportAllFromCommit(commitID)
  }

  def exportAllFromCommit(commitID: String): Unit = {
    val ns = apiClient.getStorageNamespace(repoName)
    val df = LakeFSContext.newDF(spark, repoName, commitID)

    val tableName = randPrefix() + "_commit"
    df.createOrReplaceTempView(tableName)

    // pin Exporter field to avoid serialization
    val dst = dstRoot
    val actionsDF = spark.sql(s"SELECT 'copy' as action, * FROM ${tableName}")

    actOnActions(ns, dst, commitID, actionsDF)
  }

  final private val maxLoggedErrors = 10000
  private def export(round: Int, ns: String, rel: String, commitID: String, actionsDF: DataFrame) : Boolean =  {
    import spark.implicits._

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val serializedConf = new SerializableWritable(hadoopConf)
    val f = filter
    val errs = actionsDF.map(
      row =>
        Exporter.handleRow(f, round, ns, rel, serializedConf, row)
    ).filter(status => !status.success)

    if (errs.isEmpty){
      return true
    }

    writeSummaryFile(false, commitID, errs.take(maxLoggedErrors).map(s => s.msg).reduce(_+"\n"+_))
    false
  }

  def exportFrom(branch: String, prevCommitID: String): Unit = {
    val commitID = apiClient.getBranchHEADCommit(repoName, branch)
    val ns = apiClient.getStorageNamespace(repoName)

    val newDF = LakeFSContext.newDF(spark, repoName, commitID)
    val prevDF = LakeFSContext.newDF(spark, repoName, prevCommitID)

    val newTableName =  randPrefix() + "_new_commit"
    val prevTableName =  randPrefix() + "_prev_commit"
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

    actOnActions(ns, dst, commitID, actionsDF)
  }

  final private val successMsg = "Export completed successfully!"
  private def actOnActions(ns: String, dst: String, commitID: String, actionsDF: DataFrame): Unit = {
    for( i <- 1 to filter.rounds()){
      if (!export(i, ns, dst, commitID, actionsDF)) {
        // failed to export files, don't export next round
        return
      }
    }

    writeSummaryFile(true, commitID, successMsg)
  }

  private def writeSummaryFile(success: Boolean, commitID: String, content : String) = {
    val suffix = if(success) "SUCCESS" else "FAILURE"
    val dstPath = resolveURL(new URL(dstRoot), s"EXPORT_${commitID}_${suffix}")
    val dstFS = dstPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val stream = dstFS.create(dstPath)
    stream.writeChars(content)
    stream.close()
  }

  final private val prefixLen = 8
  private def randPrefix() : String = {
    val gen = Random.alphanumeric.dropWhile(_.isDigit)
    gen.take(prefixLen).mkString("")
  }
}

object Exporter {
  private def handleRow(filter: KeyFilter, round:Int, ns: String, rootDst: String, serializedConf: SerializableWritable[Configuration], row: Row) : ExportStatus =  {
    val action = row(0)
    val key = row(1).toString()
    val address = row(2).toString()

    if (filter.roundForKey(key) != round){
      // skip this round
      return null
    }

    val conf = serializedConf.value
    val srcPath = resolveURL(new URL(ns), address)
    val dstPath = resolveURL(new URL(rootDst), key)

    val dstFS = dstPath.getFileSystem(conf)

    action match {
      case "delete" => {
        try {
          dstFS.delete(dstPath, false)
          null
        } catch {
          case e : (IOException) =>  ExportStatus(dstPath.toString, success = false, s"Unable to delete file ${dstPath.toString}: ${e.toString}")
        }
      }

      case "copy" =>{
        try {
          org.apache.hadoop.fs.FileUtil.copy(
            srcPath.getFileSystem(conf),
            srcPath,
            dstFS,
            dstPath,
            false,
            conf)
          null
        } catch  {
          case e : (FileNotFoundException) =>  ExportStatus(dstPath.toString, success = true, s"Unable to copy file ${dstPath.toString} from source ${srcPath.toString} since source file is missing: ${e.toString}")
          case e : (IOException) =>  ExportStatus(dstPath.toString, success = false, s"Unable to copy file ${dstPath.toString} from source ${srcPath.toString}: ${e.toString}")
        }
      }
    }
  }

  private def resolveURL(baseUrl :URL, extraPath: String): Path = {
    val uri = baseUrl.toURI
    val newPath: String = uri.getPath + '/' + extraPath

    new Path(uri.resolve(newPath))
  }
}
