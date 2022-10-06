package io.treeverse.clients

import org.apache.hadoop.conf.Configuration

import scala.util.control.Breaks._
import org.apache.hadoop.tools.DistCp
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.net.URI

class GCBackupAndRestore {}

object GCBackupAndRestore {
  val S3AccessKeyName = "fs.s3a.access.key"
  val S3SecretKeyName = "fs.s3a.secret.key"
  val AzureStorageAccountKeyName = "fs.azure.account.key"
  lazy val spark = SparkSession.builder().appName("GCBackupAndRestore").getOrCreate()
  import spark.implicits._

  def constructAbsoluteObjectPaths(
      objectsRelativePathsDF: DataFrame,
      srcNamespace: String,
      storageType: String
  ): Dataset[String] = {
    var storageNSForFS = ApiClient
      .translateURI(URI.create(srcNamespace), storageType)
      .normalize()
      .toString
    if (!storageNSForFS.endsWith("/")) {
      storageNSForFS += "/"
    }

    objectsRelativePathsDF
      .select("address")
      .as[String]
      .flatMap(x => StorageUtils.concatKeysToStorageNamespace(Seq(x), storageNSForFS, storageType))
  }

  def validateAndParseHadoopConfig(
      hc: Configuration,
      storageType: String
  ): Array[(String, String)] = {
    storageType match {
      case StorageUtils.StorageTypeS3 =>
        val hadoopProps =
          HadoopUtils.getHadoopConfigurationValues(hc, S3AccessKeyName, S3SecretKeyName)
        if (
          hadoopProps.iterator
            .filter(x => S3AccessKeyName.equals(x._1))
            .length != 1
        ) {
          Console.err.println(
            "missing required hadoop property. " + S3AccessKeyName
          )
          System.exit(1)
        }
        if (
          hadoopProps.iterator
            .filter(x => S3SecretKeyName.equals(x._1))
            .length != 1
        ) {
          Console.err.println(
            "missing required hadoop property. " + S3SecretKeyName
          )
          System.exit(1)
        }
        hadoopProps
      case StorageUtils.StorageTypeAzure =>
        val hadoopProps = HadoopUtils.getHadoopConfigurationValues(hc, AzureStorageAccountKeyName)
        if (hadoopProps == null || hadoopProps.length != 1) {
          Console.err.println(
            "missing required hadoop property. " + AzureStorageAccountKeyName
          )
          System.exit(1)
        }
        hadoopProps
    }
  }

  // Construct a DistCp command of the form:
  // `hadoop distcp -D<per storage credentials> -f absoluteAddressesTextFilePath dstNamespaceForHadoopFs`
  // example for command format https://docs.lakefs.io/integrations/distcp.html
  def constructDistCpCommand(
      hadoopProps: Array[(String, String)],
      absoluteAddressesTextFilePath: String,
      dstNamespaceForHadoopFs: String
  ): Array[String] = {
    hadoopProps.map((prop) => "-D" + prop._1 + "=" + prop._2) ++
      Seq(
        // -f option copies the files listed in the file after the -f option
        "-f",
        absoluteAddressesTextFilePath,
        dstNamespaceForHadoopFs
      )
  }

  // Find the path of the first txt file under a prefix.
  def getTextFileLocation(prefix: String, hc: Configuration): String = {
    val fs = FileSystem.get(URI.create(prefix), hc)
    val dirIterator = fs.listFiles(new Path(prefix), false)
    var textFilePath = ""
    breakable(
      while (dirIterator.hasNext) {
        val curFile = dirIterator.next()
        val curPath = curFile.getPath.toString
        if (curPath.endsWith("txt")) {
          textFilePath = curPath
          break
        }
      }
    )
    textFilePath
  }

  /** Required arguments are the following:
   *  1. address of parquet that includes the relative paths of files to backup or restore, created by a gc run
   *  2. src: the namespace to backup or restore objects from/to (i.e. repo storage namespace, or an external location compatibly)
   *  3. backup/restore destination: the namespace to backup or restore objects from/to (i.e. an external location or repo storage namespace compatibly)
   *  4. Object storage type: "s3" or "azure"
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      Console.err.println(
        "Usage: ... <objects list location> <src namespace> <dst namespace> <storage type>"
      )
      System.exit(1)
    }
    val relativeAddressesLocation = args(0)
    val srcNamespace = args(1)
    val dstNamespace = args(2)
    val storageType = args(3)

    val hc = spark.sparkContext.hadoopConfiguration
    val hadoopProps = validateAndParseHadoopConfig(hc, storageType)

    val relativeAddressesLocationForHadoopFs =
      ApiClient.translateURI(URI.create(relativeAddressesLocation), storageType).toString
    val dstNamespaceForHadoopFs =
      ApiClient.translateURI(URI.create(dstNamespace), storageType).toString
    print("translated dstNamespace: " + dstNamespaceForHadoopFs + "\n")

    val objectsRelativePathsDF = spark.read.parquet(relativeAddressesLocationForHadoopFs)
    val objectsAbsolutePathsDF =
      constructAbsoluteObjectPaths(objectsRelativePathsDF, srcNamespace, storageType)
    // We assume that there are write permissions to the dst namespace and therefore creating intermediate output there.
    val absoluteAddressesLocation =
      dstNamespaceForHadoopFs + "/_gc-backup-restore/absolute_addresses/"
    print("absoluteAddressesLocation: " + absoluteAddressesLocation + "\n")
    // This application uses distCp to copy files. distCp can copy a list of files in a given input text file. therefore,
    // we write the absolute file paths into a text file rather than a parquet.
    objectsAbsolutePathsDF
      .repartition(1)
      .write
      .text(absoluteAddressesLocation)

    // Spark writes two files under absoluteAddressesLocation, a _SUCCESS file and the actual txt file that has dynamic name.
    val absoluteAddressesTextFilePath = getTextFileLocation(absoluteAddressesLocation, hc)
    print("txtFilePath: " + absoluteAddressesTextFilePath + "\n")

    val distCpCommand =
      constructDistCpCommand(hadoopProps, absoluteAddressesTextFilePath, dstNamespaceForHadoopFs)
    DistCp.main(distCpCommand)
  }
}
