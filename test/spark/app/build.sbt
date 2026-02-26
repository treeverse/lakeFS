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
val scala213Version = "2.13.14"

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
        "org.slf4j" % "slf4j-api" % "1.7.36" % "provided",
      ),
      target := { baseDirectory.value / "target" / s"${baseName}-${buildType.name}" }
    )

lazy val proj24 = generateProject(new BuildType("246", scala211Version, "2.4.6", "2.7.7"))
lazy val proj31 = generateProject(new BuildType("311", scala212Version, "3.1.1", "2.7.7"))
lazy val proj400 = generateProject(new BuildType("400", scala213Version, "4.0.0", "3.4.1"))
lazy val proj411 = generateProject(new BuildType("411", scala213Version, "4.1.1", "3.4.1"))

lazy val root = (project in file("."))
  .aggregate(proj24, proj31, proj400, proj411)
  .settings(
      compile / skip := true,
      publish / skip := true,
  )


lazy val commonSettings = Seq(
  version := projectVersion,
  javacOptions ++= {
    if (scalaBinaryVersion.value == "2.13") Seq("-source", "1.8")
    else Seq("-source", "1.8", "-target", "1.8")
  },
  scalacOptions ++= {
    if (scalaBinaryVersion.value == "2.13") Seq("-release", "8", "-deprecation", "-feature")
    else Seq("-target:jvm-1.8", "-deprecation", "-feature")
  }
)

lazy val sharedSettings = commonSettings

ThisBuild / organization := "io.lakefs"
ThisBuild / organizationName := "Treeverse Labs"
ThisBuild / organizationHomepage := Some(url("http://treeverse.io"))
ThisBuild / description := "Spark test app for working with lakeFS"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/treeverse/lakeFS"))
