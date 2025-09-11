lazy val projectVersion = "0.16.0-demo-19"
version := projectVersion
lazy val hadoopVersion = "3.3.6"
ThisBuild / isSnapshot := false
ThisBuild / scalaVersion := "2.12.12"

name := "benel-test-lakefs-spark-client"
organization := "io.lakefs"
organizationName := "Treeverse Labs"
organizationHomepage := Some(url("http://treeverse.io"))
description := "Spark client for lakeFS object metadata."
licenses := List(
  "Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
homepage := Some(url("https://lakefs.io"))

javacOptions ++= Seq("-source", "1.11", "-target", "1.8")
scalacOptions += "-target:jvm-1.8"
semanticdbEnabled := true // enable SemanticDB
semanticdbVersion := scalafixSemanticdb.revision
scalacOptions += "-Ywarn-unused-import"
Compile / PB.includePaths += (Compile / resourceDirectory).value
Compile / PB.protoSources += (Compile / resourceDirectory).value
Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
Test / logBuffered := false

// Uncomment to get accurate benchmarks with just "sbt test".
// Otherwise tell sbt to
//     "testOnly io.treeverse.clients.ReadSSTableBenchmark"
// (or similar).
//
//     Test / parallelExecution := false,

// Uncomment to get (very) full stacktraces in test:
//      Test / testOptions += Tests.Argument("-oF"),
buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "io.treeverse.clients"

enablePlugins(S3Plugin, BuildInfoPlugin)

// Required for scala 2.12.12 compatibility
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.7",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.12.7",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.12.7",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.7"
)

libraryDependencies ++= Seq(
  "io.lakefs" % "sdk" % "1.53.1",
  "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided",
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
  "org.apache.hadoop" % "hadoop-azure" % hadoopVersion % "provided",
  "org.json4s" %% "json4s-native" % "3.6.12",
  "org.rogach" %% "scallop" % "4.0.3",
  "com.azure" % "azure-core" % "1.10.0",
  "com.azure" % "azure-storage-blob" % "12.9.0",
  "com.azure" % "azure-storage-blob-batch" % "12.7.0",
  "com.azure" % "azure-identity" % "1.2.0",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.569" % "provided",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.18",
  "com.google.cloud" % "google-cloud-storage" % "2.35.0",
  // Snappy is JNI :-(.  However it does claim to work with
  // ClassLoaders, and (even more importantly!) using a preloaded JNI
  // version will probably continue to work because the C language API
  // is quite stable.  Take the version documented in DataBricks
  // Runtime 7.6, and note that it changes in 8.3 :-(
  "org.xerial.snappy" % "snappy-java" % "1.1.8.4",
  "dev.failsafe" % "failsafe" % "3.2.4",
  "com.squareup.okhttp3" % "mockwebserver" % "4.10.0" % "test",
  "xerces" % "xercesImpl" % "2.12.2" % "test",
  "org.scalatest" %% "scalatest" % "3.2.16" % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.16.0" % "test",
  "org.scalatestplus" %% "mockito-4-11" % "3.2.16.0" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.10" % "test",
  "com.lihaoyi" %% "upickle" % "1.4.0" % "test",
  "com.lihaoyi" %% "os-lib" % "0.7.8" % "test",
  "com.storm-enroute" %% "scalameter" % "0.19" % "test"
)

def rename(prefix: String) = ShadeRule.rename(prefix -> "io.lakefs.spark.shade.@0")

assembly / assemblyShadeRules := Seq(
  rename("org.apache.http.**").inAll,
  rename("scalapb.**").inAll,
  rename("com.google.protobuf.**").inAll,
  rename("com.google.common.**")
    .inLibrary("com.google.guava" % "guava" % "30.1-jre",
               "com.google.guava" % "failureaccess" % "1.0.1"
              )
    .inProject,
  rename("scala.collection.compat.**").inAll,
  rename("okio.**").inAll,
  rename("okhttp3.**").inAll,
  rename("reactor.netty.**").inAll,
  rename("reactor.util.**").inAll
)

// ===== Safe upload =====
lazy val s3PutIfAbsent = taskKey[Unit]("Upload JAR to S3 atomically with If-None-Match")

s3PutIfAbsent := {
  import sys.process._

  val bucket = "benel-public-test"
  val jarFile = (assembly / assemblyOutputPath).value
  val key = s"${name.value}/${version.value}/${(assembly / assemblyJarName).value}"
  val url = s"https://$bucket.s3.amazonaws.com/$key"
  val region = "us-east-1"

  val cmd = Seq(
    "aws","s3api","put-object",
    "--bucket", bucket,
    "--key", key,
    "--body", jarFile.getAbsolutePath,
    "--if-none-match","*",
    "--region", region
    // "--acl","public-read"
  )
  val code = Process(cmd).!
  if (code != 0) {
    val exists = Process(Seq("aws","s3api","head-object","--bucket",bucket,"--key",key,"--region",region)).! == 0
    if (exists) sys.error(s"Artifact already exists: $url")
    else sys.error(s"s3 put-object failed: $url")
  } else {
    println(s"Uploaded to S3 successfully: $url")
  }
}
// ===== End safe upload =====

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    (xs map { _.toLowerCase }) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

ThisBuild / versionScheme := Some("early-semver")
