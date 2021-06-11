import build.BuildType

val baseName = "lakefsfs"

val projectVersion = "0.1.0"

ThisBuild / resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots/"

// Spark versions 2.4.7 and 3.0.1 use different Scala versions.  Changing this is a deep
// change, so key the Spark distinction by the Scala distinction.  sbt doesn't appear to
// support other ways of changing emitted Scala binary versions using the same compiler.

// SO https://stackoverflow.com/a/60177627/192263 hints that we cannot use 2.11 here before
// this version
val scala211Version = "2.11.12"
val scala212Version = "2.12.12"

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
        // We link directly with our hadoop-lakefs in order to have access to our version of the api-client
        // the same version bundled into our filesystem, this version already shade all the right dependencies like the gson
        // package.
        // In case of using the lakefs client library, we will need to shade the required libraries by the client api as
        // the hadoop-lakefs doesn't shade the client and also use it.
        "io.lakefs" % "hadoop-lakefs-assembly" % "0.1.0-RC.0-SNAPSHOT",
        "org.apache.spark" %% "spark-sql" % buildType.sparkVersion % "provided",
        "org.apache.hadoop" % "hadoop-aws" % buildType.hadoopVersion,
        "org.apache.hadoop" % "hadoop-common" % buildType.hadoopVersion,
      ),
      target := { baseDirectory.value / "target" / s"${baseName}-${buildType.name}" }
    )

val spark2Type = new BuildType("247", scala211Version, "2.4.7", "2.7.7")
val spark3Type = new BuildType("301", scala212Version, "3.0.1", "2.7.7")

lazy val proj2 = generateProject(spark2Type)
lazy val proj3 = generateProject(spark3Type)

lazy val root = (project in file("."))
  .aggregate(proj2, proj3)
  .settings(
      compile / skip := true,
      assembly / skip := true,
      publish / skip := true,
  )


lazy val assemblySettings = Seq(
  assembly / assemblyMergeStrategy := (_ => MergeStrategy.first),
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("okio.**" -> "okio.shaded.@0").inAll,
    ShadeRule.rename("okhttp3.**" -> "okhttp3.shaded.@0").inAll,
    ShadeRule.rename("scala.collection.compat.**" -> "shadecompat.@1").inAll,
  ),
)

lazy val commonSettings = Seq(
  version := projectVersion,
  // Use an older JDK to be Spark compatible
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions += "-target:jvm-1.8"
)

lazy val sharedSettings = commonSettings ++ assemblySettings

ThisBuild / organization := "io.treeverse"
ThisBuild / organizationName := "Treeverse Labs"
ThisBuild / organizationHomepage := Some(url("http://treeverse.io"))
ThisBuild / description := "Spark test app for lakeFS filesystem."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/treeverse/lakeFS"))
