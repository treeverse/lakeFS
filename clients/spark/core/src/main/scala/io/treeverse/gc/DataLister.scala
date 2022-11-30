package io.treeverse.gc

import io.treeverse.clients.ConfigMapper
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.FileNotFoundException
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
    dataList.toDF("address", "last_modified")
  }
}

class FileDescriptor(val path: String, val lastModified: Long) extends Serializable

class convertFileStatusRemoteIterator(val remoteIterator: RemoteIterator[LocatedFileStatus])
    extends Iterator[FileDescriptor]
    with Serializable {
  override def hasNext: Boolean = remoteIterator.hasNext

  override def next(): FileDescriptor = {
    val item = remoteIterator.next()
    new FileDescriptor(item.getPath.getName, item.getModificationTime)
  }
}

class ParallelDataLister extends DataLister with Serializable {
  def listPath(configMapper: ConfigMapper, p: Path): Iterator[FileDescriptor] = {
    try {
      val fs = p.getFileSystem(configMapper.configuration)
      val it = fs.listFiles(p, false)
      new convertFileStatusRemoteIterator(it)
    } catch {
      case _: FileNotFoundException => Iterator.empty
    }
  }

  override def listData(configMapper: ConfigMapper, path: Path): DataFrame = {
    import spark.implicits._

    val slices = listPath(configMapper, path)
    val objectsUDF = udf((sliceId: String) => {
      val slicePath = new Path(path, sliceId)
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
