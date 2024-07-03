lazy val projectVersion = "0.14.1"
version := projectVersion
lazy val hadoopVersion = "3.2.1"
ThisBuild / isSnapshot := false
ThisBuild / scalaVersion := "2.12.12"

name := "lakefs-spark-client"
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

libraryDependencies ++= Seq(
  "io.lakefs" % "sdk" % "1.0.0",
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
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.194" % "provided",
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
  // Test with an up-to-date fasterxml.
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2" % "test",
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

s3Upload / mappings := Seq(
  (assembly / assemblyOutputPath).value ->
    s"${name.value}/${version.value}/${(assembly / assemblyJarName).value}"
)
s3Upload / s3Host := "treeverse-clients-us-east.s3.amazonaws.com"
s3Upload / s3Progress := true

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
