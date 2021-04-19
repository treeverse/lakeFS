package io.treeverse.clients.examples

import java.net.URI
import java.nio.charset.Charset
import java.io.OutputStreamWriter
import scala.collection.JavaConverters._
import scala.util.{Try,Success,Failure}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.SDKGlobalConfiguration

import org.apache.hadoop.fs
import org.apache.hadoop.fs.s3a
import org.apache.hadoop.conf.Configuration

import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.S3Client
import scala.collection.immutable.Stream

/** Exercise an underlying FileSystem. */
class UnderlyingFS(s3aFS: s3a.S3AFileSystem, bucket: String) {
  val utf8 = Charset.forName("UTF-8")

  def upload(key: String, contents: String) = {
    val path = new fs.Path("s3a", bucket, key)
    val os = s3aFS.create(path)
    os.write(contents.getBytes(utf8))
    os.close()
  }

  def download(key: String): String = {
    val path = new fs.Path("s3a", bucket, key)
    val is = s3aFS.open(path, 16384)
    val bytes = Stream.continually({
      val buf = new Array[Byte](10)
      val len = is.read(buf)
      if (len == -1) null else buf.slice(0, len)
    }).takeWhile(buf => buf != null)
      .fold(new Array[Byte](0))(_ ++ _)
    new String(bytes)
  }
}

/** Direct access to the object store of that FileSystem. */
class ObjectStore(val s3: S3Client, val bucket: String) {
  /**
    * @return keys of all objects that start with prefix.
    * 
    * @bug no support for paging.
    */
  def list(prefix: String): Seq[String] = {
    val req = ListObjectsV2Request.builder
      .bucket(bucket)
      .prefix(prefix)
      .build
    val res = s3.listObjectsV2(req)
    res.contents.asScala.map(_.key)
  }
}

object S3AUser extends App {
  def makeS3A(conf: Configuration, bucket: String): s3a.S3AFileSystem = {
    fs.FileSystem.get(new URI("s3a", bucket, "", ""), conf) match {
      case s3aFS: s3a.S3AFileSystem => s3aFS
      case _ => throw new ClassCastException
    }
  }

  override def main(args: Array[String]) {
    var numFailures = 0

    if (args.length != 1) {
      Console.err.println("Usage: ... s3://bucket/prefix/to/write")
      System.exit(1)
    }


    // Not running as part of Spark, setup a similar Hadoop configuration.

    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")

    val conf = new Configuration(true)
    // conf.set("fs.s3a.aws.credentials.provider", """com.amazonaws.auth.profile.ProfileCredentialsProvider""")
    //    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.ProfileCredentialsProvider,org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider")

    conf.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
    conf.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"))
    conf.set("fs.s3a.custom.signers", "AWS4SignerType")

    val region = System.getenv("AWS_REGION")
    if (region != null) {
      conf.set("fs.s3a.region", region)
      conf.set("fs.s3a.endpoint", s"s3.${region}.amazonaws.com") // Otherwise it tries host-based addressing and fails
    }

    Try(new URI(args(0))) match {
      case Failure(e) => {
        Console.err.printf("parse URI: %s", e)
        System.exit(1)
      }
      case Success(baseURI) => {
        if (baseURI.getScheme != "s3") {
          Console.err.printf("got scheme %s but can only handle s3\n", baseURI.getScheme)
          System.exit(1)
        }
        val bucket = baseURI.getHost
        val prefix = baseURI.getPath

        val s3aFS = makeS3A(conf, bucket)
        val fs = new UnderlyingFS(s3aFS, bucket)

        val s3 = S3Client.builder.build
        val store = new ObjectStore(s3, bucket)

        fs.upload(prefix + "/abc", "quick brown fox")
        fs.upload(prefix + "/d/e/f/xyz", "foo bar")

        val abcContents = fs.download(prefix + "/abc")
        if (abcContents != "quick brown fox") {
          Console.err.println(s"""got /abc = "${abcContents}" (len ${abcContents.size})""")
          numFailures += 1
        }
        val xyzContents = fs.download(prefix + "/d/e/f/xyz")
        if (xyzContents != "foo bar") {
          Console.err.println(s"""got /d/e/f/xyz = "${xyzContents}" (len ${xyzContents.size})""")
          numFailures += 1
        }

        val prefix2 = prefix.stripPrefix("/") // S3 keys don't have that leading slash.
        val listing = store.list(prefix2).map(_.stripPrefix(prefix2)).toList
        val expected = scala.List("abc", "d/e/f/xyz")
        val tooMany = listing diff expected
        if (!tooMany.isEmpty) {
          Console.err.printf("unexpected objects on S3: %s\n", tooMany)
          numFailures += 1
        }
        val tooFew = expected diff listing
        if (!tooFew.isEmpty) {
          Console.err.printf("missing objects on S3: %s\n", tooFew)
          numFailures += 1
        }

        s3aFS.close()
      }
    }

    System.exit(if (numFailures > 0) 1 else 0)
  }
}
