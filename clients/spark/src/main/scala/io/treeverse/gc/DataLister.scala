package io.treeverse.gc

import io.treeverse.clients.ConfigMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/** List all the files under a given path.
 */
abstract class DataLister {
  @transient lazy val spark: SparkSession = SparkSession.active

  def listData(configMapper: ConfigMapper, path: Path, parallelism: Int): DataFrame
}

class NaiveDataLister extends DataLister {
  override def listData(configMapper: ConfigMapper, path: Path, parallelism: Int): DataFrame = {
    import spark.implicits._
    val fs = path.getFileSystem(configMapper.configuration)
    val dataIt = fs.listFiles(path, false)
    val dataList = new ListBuffer[(String, Long)]()
    while (dataIt.hasNext) {
      val fileStatus = dataIt.next()
      dataList += ((fileStatus.getPath.getName, fileStatus.getModificationTime))
    }
    dataList.toSeq.toDF("base_address", "last_modified").repartition(parallelism)
  }
}

class FileDescriptor(val path: String, val lastModified: Long) extends Serializable

class ParallelDataLister extends DataLister with Serializable {
  // listDirectChildrenPaths returns the path names of the direct children (files and directories) of p.
  private def listDirectChildrenPaths(configMapper: ConfigMapper, p: Path): Iterator[String] = {
    val fs = p.getFileSystem(configMapper.configuration)
    if (!fs.exists(p)) return Iterator.empty
    val it = fs.listStatusIterator(p)
    new Iterator[String] {
      override def hasNext: Boolean = it.hasNext

      override def next(): String = it.next().getPath.getName
    }
  }

  // listFilesDeep returns FileDescriptors with paths relative to p for all leaf files under p.
  // Uses makeQualified so the URI scheme (s3 vs s3a) matches what listFiles returns, making stripPrefix reliable.
  // When p is a plain file (e.g. a legacy staged-object directly under data/), using its name (instead of stripPrefix).
  private def listFilesDeep(
      configMapper: ConfigMapper,
      p: Path
  ): Iterator[FileDescriptor] = {
    val fs = p.getFileSystem(configMapper.configuration)
    val it =
      try {
        fs.listFiles(p, true)
      } catch {
        case _: java.io.FileNotFoundException => return Iterator.empty
      }
    val qualP = fs.makeQualified(p).toString
    val base = qualP + "/"
    new Iterator[FileDescriptor] {
      override def hasNext: Boolean = it.hasNext

      override def next(): FileDescriptor = {
        val s = it.next()
        val path = s.getPath.toString
        val rel =
          if (path == qualP) p.getName
          else {
            val r = path.stripPrefix(base)
            require(r != path, s"path $path is not under $base")
            r
          }
        new FileDescriptor(rel, s.getModificationTime)
      }
    }
  }

  override def listData(configMapper: ConfigMapper, path: Path, parallelism: Int): DataFrame = {
    import spark.implicits._
    // List direct children of data/ (shard or partition directories) as parallelism units.
    val sliceIds = listDirectChildrenPaths(configMapper, path)
    val objectsPath = if (path.toString.endsWith("/")) path.toString else path.toString + "/"
    // udf require serializable string and not Path
    val objectsUDF = udf((sliceId: String) => {
      // WA for https://issues.apache.org/jira/browse/HDFS-14762
      val slicePath = new Path(objectsPath + sliceId)
      listFilesDeep(configMapper, slicePath).toSeq.map(s => (s.path, s.lastModified))
    })

    sliceIds.toSeq
      .toDF("slice_id")
      .repartition(parallelism)
      .withColumn("udf", explode(objectsUDF(col("slice_id"))))
      .withColumn("base_address", concat(col("slice_id"), lit("/"), col("udf._1")))
      .withColumn("last_modified", col("udf._2"))
      .select("base_address", "last_modified")
  }
}
