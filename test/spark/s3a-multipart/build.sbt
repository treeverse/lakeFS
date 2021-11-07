val baseName = "multipart"

val projectVersion = "0.1.0"

lazy val p = (project in file("."))
  .settings(
    // Use an older JDK to be Spark compatible
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions += "-target:jvm-1.8",
    scalaVersion := "2.12.12",
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-aws" % "2.7.7",
      "org.apache.hadoop" % "hadoop-common" % "2.7.7",
      "software.amazon.awssdk" % "s3" % "2.15.15",
    ),
  )

ThisBuild / organization := "io.lakefs"
ThisBuild / organizationName := "Treeverse Labs"
ThisBuild / organizationHomepage := Some(url("http://lakefs.io"))
ThisBuild / description := "S3A multipart upload test app for checking lakeFS"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/treeverse/lakeFS"))
