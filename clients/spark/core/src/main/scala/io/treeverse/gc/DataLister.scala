package io.treeverse.gc

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/** List all the files under a given path.
 */
trait DataLister {
  def listData(spark: SparkSession, path: Path): DataFrame
}

class NaiveDataLister extends DataLister {
  override def listData(spark: SparkSession, path: Path): DataFrame = {
    import spark.implicits._
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val dataIt = fs.listFiles(path, false)
    val dataList = new ListBuffer[(String, Long)]()
    while (dataIt.hasNext) {
      val fileStatus = dataIt.next()
      dataList += ((fileStatus.getPath.toString, fileStatus.getModificationTime()))
    }
    dataList.toDF("address", "last_modified")
  }
}
