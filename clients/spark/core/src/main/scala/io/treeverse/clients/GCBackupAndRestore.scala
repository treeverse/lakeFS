package io.treeverse.clients

import org.apache.hadoop.conf.Configuration

import scala.util.control.Breaks._
import org.apache.hadoop.tools.DistCp
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

class GCBackupAndRestore {}

/**
Example usage:
S3 -
  spark-submit --class io.treeverse.clients.GCBackupAndRestore \
  -c spark.hadoop.fs.s3a.access.key=#### \
  -c spark.hadoop.fs.s3a.secret.key=#### \
  lakefs-spark-client-312-hadoop3-assembly-0.4.0.jar \
  s3a://my-bucket/repo/_lakefs/retention/gc/addresses/mark_id=1/part-00057-36bb66f0-6a38-47b4-86cb-0e8718dfacfe.c000.snappy.parquet s3://my-bucket/repo/ s3://external_location/gc_backup s3

Azure -
  spark-submit --packages org.apache.hadoop:hadoop-azure:3.2.1 --class io.treeverse.clients.GCBackupAndRestore \
  -c spark.hadoop.fs.azure.account.key.<storage_account>.dfs.core.windows.net=#### \
  lakefs-spark-client-312-hadoop3-assembly-0.4.0.jar \
  https://my-bucket/repo/_lakefs/retention/gc/addresses/mark_id=1/part-00146-55e42537-8c0e-4613-981f-c13a9be8f55b.c000.snappy.parquet https://my-bucket/repo/ https://external_location/gc_backup azure
 */
object GCBackupAndRestore {
  val S3AccessKeyName = "fs.s3a.access.key"
  val S3SecretKeyName = "fs.s3a.secret.key"
  val AzureStorageAccountKeyName = "fs.azure.account.key"
  lazy val spark = SparkSession.builder().appName("GCBackupAndRestore").getOrCreate()

  /** Validates that args are the following:
   *  1. address of parquet that includes the relative paths of files to backup or restore, created by a gc run
   *     2. src: the namespace to backup or restore objects from/to (i.e. repo storage namespace, or an external location compatibly)
   *     3. backup/restore destination: the namespace to backup or restore objects from/to (i.e. an external location or repo storage namespace compatibly)
   *     4. Object storage type: "s3" or "azure"
   */
  def validateArgs(args: Array[String]): Unit = {
    if (args.length != 4) {
      Console.err.println(
        "Usage: ... <objects list location> <src namespace> <dst namespace> <storage type>"
      )
      System.exit(1)
    }

    val objectsListPath = args(0)
    if (!objectsListPath.endsWith("parquet")) {
      Console.err.println(
        "<objects list path> must be of type parquet"
      )
      System.exit(1)
    }

    val storageType = args(3)
    if (!storageType.equals(StorageUtils.StorageTypeS3) && !storageType.equals(StorageUtils.StorageTypeAzure)) {
      Console.err.println(
        "Invalid <storage type>, must be either \"s3\" or \"azure\""
      )
      System.exit(1)
    }
  }

  def constructAbsoluteObjectPaths(objectsRelativePathsDF: DataFrame, srcNamespace: String, storageType: String): DataFrame = {
    var storageNSForHadoopFS = ApiClient
      .translateURI(URI.create(srcNamespace), storageType)
      .normalize()
      .toString
    if (!storageNSForHadoopFS.endsWith("/")) {
      storageNSForHadoopFS += "/"
    }
    import spark.implicits._
    val objectsRelativePathsSeq = objectsRelativePathsDF
      .select("address")
      .map(_.getString(0))
      .collect()
      .toSeq

    StorageUtils.concatKeysToStorageNamespace(objectsRelativePathsSeq, storageNSForHadoopFS, storageType).toDF()
  }

  def validateAndParseHadoopConfig(hc: Configuration, storageType: String): Array[(String, String)] = {
    var hadoopProps: Array[(String, String)] = null
    storageType match {
      case StorageUtils.StorageTypeS3 =>
        hadoopProps = HadoopUtils.getHadoopConfigurationValues(hc, S3AccessKeyName, S3SecretKeyName)
        if (hadoopProps == null || hadoopProps.length != 2) {
          Console.err.println(
            "missing required hadoop properties. " + S3AccessKeyName + " or " + S3SecretKeyName
          )
          System.exit(1)
        }
      case StorageUtils.StorageTypeAzure =>
        hadoopProps = HadoopUtils.getHadoopConfigurationValues(hc, AzureStorageAccountKeyName)
        if (hadoopProps == null || hadoopProps.length != 1) {
          Console.err.println(
            "missing required hadoop property. " + AzureStorageAccountKeyName
          )
          System.exit(1)
        }
    }
    hadoopProps
  }

  //    DistCp.main(Array("-f", absoluteAddressesTextFilePath, dstNamespaceForHadoopFs))
  // example for command format https://docs.lakefs.io/integrations/distcp.html
  def constructDistCpCommand(hadoopProps: Array[(String, String)], absoluteAddressesTextFilePath: String, dstNamespaceForHadoopFs: String): Array[String] = {
    var res: Array[String] = Array()
    for (prop <- hadoopProps) {
      val formattedProp = "-D" + prop._1 + "=" + prop._2
      res = res :+ formattedProp
    }
    // -f option copies the files listed in the file after the -f option
    res = res :+ "-f"
    res = res :+ absoluteAddressesTextFilePath
    res = res :+ dstNamespaceForHadoopFs
    res
  }

  def main(args: Array[String]): Unit = {
    validateArgs(args)
    val relativeAddressesLocation = args(0)
    val srcNamespace = args(1)
    val dstNamespace = args(2)
    val storageType = args(3)

    val hc = spark.sparkContext.hadoopConfiguration
    val hadoopProps = validateAndParseHadoopConfig(hc, storageType)

    val relativeAddressesLocationForHadoopFs = ApiClient.translateURI(URI.create(relativeAddressesLocation), storageType).toString
    val dstNamespaceForHadoopFs = ApiClient.translateURI(URI.create(dstNamespace), storageType).toString
    print("translated dstNamespace: " + dstNamespaceForHadoopFs + "\n")

    val objectsRelativePathsDF = spark.read.parquet(relativeAddressesLocationForHadoopFs)
    val objectsAbsolutePathsDF = constructAbsoluteObjectPaths(objectsRelativePathsDF, srcNamespace, storageType)
    // We assume that there are write permissions to the dst namespace and therefore creating intermediate output there.
    val absoluteAddressesLocation = dstNamespaceForHadoopFs + "/_gc-backup-restore/absolute_addresses/"
    print("absoluteAddressesLocation: " + absoluteAddressesLocation + "\n")
    // This application uses distCp to copy files. distCp can copy a list of files in a given input text file. therefore,
    // we write the absolute file paths into a text file rather than a parquet.
    objectsAbsolutePathsDF
      .repartition(1)
      .write
      .text(absoluteAddressesLocation)

    // Spark writes two files under absoluteAddressesLocation, a _SUCCESS file and the actual txt file that has dynamic
    // name. iterate the files in the absoluteAddressesLocation and find the path of the txt files that includes the
    // list of absolute addresses.
    val fs = FileSystem.get(URI.create(absoluteAddressesLocation), hc)
    val dirIterator = fs.listFiles(new Path(absoluteAddressesLocation), false)
    var absoluteAddressesTextFilePath = ""
    breakable(
      while (dirIterator.hasNext) {
        val curFile = dirIterator.next()
        val curPath = curFile.getPath.toString
        if (curPath.endsWith("txt")) {
          absoluteAddressesTextFilePath = curPath
          break
        }
      }
    )
    print("txtFilePath: " + absoluteAddressesTextFilePath + "\n")

    val distCpCommand = constructDistCpCommand(hadoopProps, absoluteAddressesTextFilePath, dstNamespaceForHadoopFs)
    DistCp.main(distCpCommand)
  }
}
