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
  def listData(configMapper: ConfigMapper, path: Path): DataFrame
}

class NaiveDataLister extends DataLister {
  override def listData(configMapper: ConfigMapper, path: Path): DataFrame = {
    import spark.implicits._
    val fs = path.getFileSystem(configMapper.configuration)
    val dataIt = fs.listFiles(path, false)
    val dataList = new ListBuffer[(String, Long)]()
    while (dataIt.hasNext) {
      val fileStatus = dataIt.next()
      dataList += ((fileStatus.getPath.getName, fileStatus.getModificationTime))
    }
    dataList.toDF("base_address", "last_modified")
  }
}

class FileDescriptor(val path: String, val lastModified: Long) extends Serializable

class ParallelDataLister extends DataLister with Serializable {
  private def listPath(configMapper: ConfigMapper, p: Path): Iterator[FileDescriptor] = {
    val fs = p.getFileSystem(configMapper.configuration)
    if (!fs.exists(p)) {
      return Iterator.empty
    }
    val it = fs.listStatusIterator(p)
    new Iterator[FileDescriptor] with Serializable {
      override def hasNext: Boolean = it.hasNext

      override def next(): FileDescriptor = {
        val item = it.next()
        new FileDescriptor(item.getPath.getName, item.getModificationTime)
      }
    }
  }

  override def listData(configMapper: ConfigMapper, path: Path): DataFrame = {
    import spark.implicits._
    val slices = listPath(configMapper, path)
    val objectsPath = if (path.toString.endsWith("/")) path.toString else path.toString + "/"
    // udf require serializable string and not Path
    val objectsUDF = udf((sliceId: String) => {
      // WA for https://issues.apache.org/jira/browse/HDFS-14762
      val slicePath = new Path(objectsPath + sliceId)
      listPath(configMapper, slicePath).toSeq
        .map(s => (s.path, s.lastModified))
    })
    val objectsDF = slices
      .map(_.path)
      .toSeq
      .toDF("slice_id")
      .withColumn("udf", explode(objectsUDF(col("slice_id"))))
      .withColumn("base_address", concat(col("slice_id"), lit("/"), col("udf._1")))
      .withColumn("last_modified", col("udf._2"))
      .select("base_address", "last_modified")
    objectsDF
  }
}
