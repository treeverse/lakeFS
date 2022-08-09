package io.lakefs.tests

import java.net.URI

import org.apache.hadoop.fs
import org.apache.hadoop.conf.Configuration
import com.amazonaws.SDKGlobalConfiguration

import scala.util.{Try, Success, Failure}

object S3AMultipart extends App {
  def newRandom() = new scala.util.Random(17)

  override def main(args: Array[String]) {
    val partSize = 5 << 20 // Must be >= 5MiB on AWS S3.
    val fileSize = 2 * partSize
    val writeSize = 1 << 18

    if (args.length != 1) {
      Console.err.println("Usage: ... s3://bucket/path/to/object")
      System.exit(1)
    }
    val path = args(0)

    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")

    val conf = new Configuration(true)
    conf.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
    conf.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"))
    conf.set("fs.s3a.custom.signers", "AWS4SignerType")
    conf.set("fs.s3a.multipart.size", s"${5 << 20}")
    conf.set("fs.s3a.multipart.threshold", s"${5 << 20}")

    val region = System.getenv("AWS_REGION")
    if (region != null) {
      conf.set("fs.s3a.region", region)
      conf.set("fs.s3a.endpoint",
               s"s3.${region}.amazonaws.com"
              ) // Otherwise it tries host-based addressing and fails
    }

    val endpoint = System.getenv("ENDPOINT")
    if (endpoint != null) {
      conf.set("fs.s3a.endpoint", endpoint)
    }

    val uri = try {
      new URI(args(0))
    } catch {
      case e: (Any) => {
        Console.err.printf("parse URI %s: %s\n", args(0), e)
        System.exit(1)
        null
      }
    }

    val filesystem = fs.FileSystem.get(uri, conf)

    def asBytes(r: scala.util.Random, size: Int): Iterator[Array[Byte]] = {
      def getNext() = {
        val bytes = new Array[Byte](size)
        r.nextBytes(bytes)
        bytes
      }
      Iterator.continually(getNext())
    }

    val up = filesystem.create(new fs.Path(path))

    val upRand = newRandom()
    asBytes(upRand, writeSize).take(fileSize / writeSize).foreach(b => up.write(b, 0, b.length))
    up.close()

    val down = filesystem.open(new fs.Path(path))
    val actualBytes = Iterator.continually(down.read()).takeWhile(_ >= 0).map(_.toByte)

    val expectedBytes = asBytes(newRandom(), writeSize).flatten

    val diffs = (actualBytes zip expectedBytes).
      zipWithIndex.
      filter({ case (((a, b), i)) => a != b }).
      take(10).
      toList

    if (! diffs.isEmpty) {
      val diffsText = diffs.map({ case ((a, b), i) => s"${a} != ${b} @${i}" })
      Console.err.println(s"Downloaded other bytes than uploaded; first diffs ${diffsText}")
      System.exit(1)
    }
  }
}
