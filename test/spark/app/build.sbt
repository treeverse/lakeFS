import build.BuildType

val baseName = "sonnets"

val projectVersion = "0.1.0"

// Spark versions 2.4.7 and 3.0.1 use different Scala versions.  Changing this is a deep
// change, so key the Spark distinction by the Scala distinction.  sbt doesn't appear to
// support other ways of changing emitted Scala binary versions using the same compiler.

// SO https://stackoverflow.com/a/60177627/192263 hints that we cannot use 2.11 here before
// this version
val scala211Version = "2.11.12"
val scala212Version = "2.12.13"

def settingsToCompileIn() = {
  Seq(
    Compile / scalaSource := (ThisBuild / baseDirectory).value / "src" / "main" / "scala",
  )
}

def generateProject(buildType: BuildType) =
  Project(s"${baseName}-${buildType.name}", file(s"target/${baseName}-${buildType.name}"))
    .settings(
      sharedSettings,
      settingsToCompileIn(),
      scalaVersion := buildType.scalaVersion,
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-sql" % buildType.sparkVersion % "provided",
        "org.apache.hadoop" % "hadoop-aws" % buildType.hadoopVersion,
        "org.apache.hadoop" % "hadoop-common" % buildType.hadoopVersion,
      ),
      target := { baseDirectory.value / "target" / s"${baseName}-${buildType.name}" }
    )

lazy val proj24 = generateProject(new BuildType("246", scala211Version, "2.4.6", "2.7.7"))
lazy val proj31 = generateProject(new BuildType("311", scala212Version, "3.1.1", "2.7.7"))

lazy val root = (project in file("."))
  .aggregate(proj24, proj31)
  .settings(
      compile / skip := true,
      publish / skip := true,
  )


lazy val commonSettings = Seq(
  version := projectVersion,
  // Use an older JDK to be Spark compatible
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature")
)

lazy val sharedSettings = commonSettings

ThisBuild / organization := "io.lakefs"
ThisBuild / organizationName := "Treeverse Labs"
ThisBuild / organizationHomepage := Some(url("http://treeverse.io"))
ThisBuild / description := "Spark test app for working with lakeFS"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/treeverse/lakeFS"))
