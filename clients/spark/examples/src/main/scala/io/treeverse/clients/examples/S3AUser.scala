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

import software.amazon.awssdk.services.s3.model.{DeleteObjectRequest, ListObjectsV2Request, PutObjectRequest}
import software.amazon.awssdk.services.s3.S3Client
import scala.collection.immutable.Stream
import scala.collection.mutable

trait Store {
  def upload(key: String, contents: String)
  def download(key: String): String
  def delete(key: String)
}

/** Exercise an underlying FileSystem. */
class UnderlyingFS(s3aFS: s3a.S3AFileSystem, bucket: String, prefix: String) extends Store {
  val utf8 = Charset.forName("UTF-8")

  def upload(key: String, contents: String) = {
    val path = new fs.Path("s3a", bucket, prefix + key)
    val os = s3aFS.create(path)
    os.write(contents.getBytes(utf8))
    os.close()
  }

  def download(key: String): String = {
    val path = new fs.Path("s3a", bucket, prefix + key)
    val is = s3aFS.open(path, 16384)
    val bytes = Stream.continually({
      val buf = new Array[Byte](10)
      val len = is.read(buf)
      if (len == -1) null else buf.slice(0, len)
    }).takeWhile(buf => buf != null)
      .fold(new Array[Byte](0))(_ ++ _)
    new String(bytes)
  }

  def delete(key: String) = {
    val path = new fs.Path("s3a", bucket, prefix + key)
    s3aFS.delete(path)
  }
}

/** Store of currently existing keys */
class KeyStore extends Store {
  val store = mutable.Map[String, String]()

  def list(): Seq[String] = store.keys.toList.sorted

  def upload(key: String, contents: String) = store.update(key, contents)

  def download(key: String): String = store.get(key).get

  def delete(key: String) = store.remove(key)
}

class CompareStore(val a: Store, val b: Store) extends Store {
  /** Upload key -> contents to each store. */
  def upload(key: String, contents: String) = {
    a.upload(key, contents)
    b.upload(key, contents)
  }

  /** Download key from each store, if different return a non-empty explanation */
  def download(key: String): String = {
    val aValue = a.download(key)
    val bValue = b.download(key)
    if (aValue != bValue) s""""${aValue}" != "${bValue}"""" else ""
  }

  /** Delete key from each store. */
  def delete(key: String) = {
    a.delete(key)
    b.delete(key)
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
    val sPrefix = prefix.stripPrefix("/")
    val req = ListObjectsV2Request.builder
      .bucket(bucket)
      .prefix(sPrefix)
      .build
    val res = s3.listObjectsV2(req)
    res.contents.asScala.map(_.key).map("/" + _.stripPrefix(sPrefix))
  }

  def delete(prefix: String, key: String) = {
    val path = s"${prefix.stripPrefix("/")}${key.stripPrefix("/")}"
    val req = DeleteObjectRequest.builder
      .bucket(bucket)
      .key(path)
      .build
    val res = s3.deleteObject(req)
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

        def failDownload(key: String, stores: Store*): String =
          stores.map(store =>
            Try(store.download(key)) match {
              case Failure(_: java.io.FileNotFoundException | _: java.util.NoSuchElementException) => null
              case Failure(e) => s"unexpected exception ${e}"
              case Success(contents) => s"value ${contents}, expected deleted"
            })
            .filter(_ != null)
            .mkString(", ")

        def checkObjectStore(store: KeyStore, objects: ObjectStore, prefix: String): String = {
          val expected = store.list()
          val actual = objects.list(prefix).toList
          val tooMany = actual diff expected
          val tooFew = expected diff actual
          Seq(
            if (tooMany.isEmpty) null else  s"""unexpected objects ${tooMany.mkString(", ")}""",
            if (tooFew.isEmpty) null else s"""missing objects ${tooFew.mkString(", ")}""",
          ).filter(_ != null).mkString("; ")
        }

        def isNonemptyString(s: Any) = s match { case s: String => s != ""; case _ => false}

        val s3aFS = makeS3A(conf, bucket)
        val fs = new UnderlyingFS(s3aFS, bucket, prefix)
        val mem = new KeyStore
        val compare = new CompareStore(fs, mem)

        val s3 = S3Client.builder.build
        val objects = new ObjectStore(s3, bucket)

        val actions = Seq(
          () => compare.upload("/abc", "quick brown fox"),
          () => compare.upload("/d/e/f/xyz", "foo bar"),
          () => checkObjectStore(mem, objects, prefix),
          () => compare.upload("/abc/g", "slow white lemming"),
          () => compare.download("/abc"),
          () => compare.download("/d/e/f/xyz"),
          () => compare.download("/abc/g"),
          () => compare.delete("/abc"),
          () => failDownload("/abc", fs, mem),
          () => compare.download("/abc/g"),
          () => checkObjectStore(mem, objects, prefix),
        )

        // TODO(ariels): Compare current state on ObjectStore after each action.
        actions.map(_()).zipWithIndex.filter({ case (res, _) => isNonemptyString(res) }).foreach({ case (err, idx) => {
          Console.err.println(s"[${idx}] ${err}\n")
          numFailures += 1
        }})

        // Clean up (_only_ known files).
        mem.list().foreach(key => objects.delete(prefix, key))

        s3aFS.close()
      }
    }

    System.exit(if (numFailures > 0) 1 else 0)
  }
}
