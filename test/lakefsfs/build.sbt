import build.BuildType

val baseName = "lakefsfs-test"

val projectVersion = "0.1.0"

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
      s3UploadSettings,
      settingsToCompileIn(),
      scalaVersion := buildType.scalaVersion,
      libraryDependencies ++= Seq(
        // We link directly with our hadoop-lakefs in order to have access to our version of the api-client
        // the same version bundled into our filesystem, this version already shade all the right dependencies like the gson
        // package.
        // In case of using the lakefs client library, we will need to shade the required libraries by the client api as
        // the hadoop-lakefs doesn't shade the client and also use it.
        "io.lakefs" % "hadoop-lakefs-assembly" % "0.1.0",
        "org.apache.spark" %% "spark-sql" % buildType.sparkVersion % "provided",
        "org.apache.hadoop" % "hadoop-aws" % buildType.hadoopVersion,
        "org.apache.hadoop" % "hadoop-common" % buildType.hadoopVersion,
      ),
      target := { baseDirectory.value / "target" / s"${baseName}-${buildType.name}" }
    ).enablePlugins(S3Plugin)

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
    ShadeRule.rename("okio.**" -> "io.lakefs.test.shade.@0").inAll,
    ShadeRule.rename("okhttp3.**" -> "io.lakefs.test.shade.@0").inAll,
    ShadeRule.rename("scala.collection.compat.**" -> "io.lakefs.test.shade.@0").inAll,
  ),
)

// Upload assembly jars to S3
lazy val s3UploadSettings = Seq(
  s3Upload / mappings := Seq(
    (assemblyOutputPath in assembly).value ->
      s"${name.value}/${version.value}/${(assemblyJarName in assembly).value}"
  ),
  s3Upload / s3Host := "treeverse-clients-us-east.s3.amazonaws.com",
  s3Upload / s3Progress  := true
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
