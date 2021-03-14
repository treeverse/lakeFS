import build.BuildType

lazy val baseName = "lakefs-spark"

lazy val projectVersion = "0.1.0-SNAPSHOT.4"
isSnapshot := true

// Spark versions 2.4.7 and 3.0.1 use different Scala versions.  Changing this is a deep
// change, so key the Spark distinction by the Scala distinction.  sbt doesn't appear to
// support other ways of changing emitted Scala binary versions using the same compiler.

// SO https://stackoverflow.com/a/60177627/192263 hints that we cannot use 2.11 here before
// this version
lazy val scala211Version = "2.11.12"
lazy val scala212Version = "2.12.12"

def settingsToCompileIn(dir: String) = {
  Seq(
    Compile / scalaSource := (ThisBuild / baseDirectory).value / dir / "src" / "main" / "scala",
    Test / scalaSource := (ThisBuild / baseDirectory).value / dir / "src" / "test" / "scala",
    Compile / resourceDirectory := (ThisBuild / baseDirectory).value / dir / "src" / "main" / "resources",
    Compile / PB.includePaths += (Compile / resourceDirectory).value,
    Compile / PB.protoSources += (Compile / resourceDirectory).value,
  )
}

def generateCoreProject(buildType: BuildType) =
  Project(s"${baseName}-client-${buildType.name}", file(s"target/core-${buildType.name}"))
    .settings(
      sharedSettings,
      settingsToCompileIn("core"),
      scalaVersion := buildType.scalaVersion,
      PB.targets := Seq(
        scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
      ),
      libraryDependencies ++= Seq("org.rocksdb" % "rocksdbjni" % "6.6.4",
        "commons-codec" % "commons-codec" % "1.15",
        "org.apache.spark" %% "spark-sql" % buildType.sparkVersion % "provided",
        "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
        "org.apache.hadoop" % "hadoop-aws" % buildType.hadoopVersion,
        "org.apache.hadoop" % "hadoop-common" % buildType.hadoopVersion,
        "org.scalaj" %% "scalaj-http" % "2.4.2",
        "org.json4s" %% "json4s-native" % "3.7.0-M8",
        "com.google.guava" % "guava" % "16.0.1",
        "com.google.guava" % "failureaccess" % "1.0.1",
      )
    )

def generateExamplesProject(buildType: BuildType) =
  Project(s"${baseName}-examples-${buildType.name}", file(s"target/examples-${buildType.name}"))
    .settings(
      sharedSettings,
      settingsToCompileIn("core"),
      scalaVersion := buildType.scalaVersion,
      libraryDependencies += "org.apache.spark" %% "spark-sql" % buildType.sparkVersion % "provided",
      mainClass in assembly := Some("io.treeverse.examples.List"),
    )

lazy val spark2Type = new BuildType("247", scala211Version, "2.4.7", "0.9.8", "2.7.7")
lazy val spark3Type = new BuildType("301", scala212Version, "3.0.1", "0.10.11", "2.7.7")

lazy val core2 = generateCoreProject(spark2Type)
lazy val core3 = generateCoreProject(spark3Type)
lazy val examples2 = generateExamplesProject(spark2Type).dependsOn(core2)
lazy val examples3 = generateExamplesProject(spark3Type).dependsOn(core3)

lazy val root = (project in file(".")).aggregate(core2, core3, examples2, examples3)

// Use an older JDK to be Spark compatible
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-release", "8", "-target:jvm-1.8")

lazy val assemblySettings = Seq(
  assembly / assemblyMergeStrategy := (_ => MergeStrategy.first),
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("org.apache.http.**" -> "org.apache.httpShaded@1").inAll,
    ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll,
    ShadeRule.rename("com.google.common.**" -> "shadegooglecommon.@1")
      .inLibrary("com.google.guava" % "guava" % "30.1-jre", "com.google.guava" % "failureaccess" % "1.0.1")
      .inProject,
    ShadeRule.rename("scala.collection.compat.**" -> "shadecompat.@1").inAll,
  ),
)

// Don't publish root project
root / publish / skip := true

lazy val commonSettings = Seq(
  version := projectVersion
)

lazy val publishSettings = Seq(
  publishTo := {
    val nexus = "https://s01.oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  // Remove all additional repository other than Maven Central from POM
  pomIncludeRepository := { _ => false },
)

lazy val sharedSettings = commonSettings ++ assemblySettings ++ publishSettings

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/treeverse/spark-client"),
    "scm:git@github.com:treeverse/spark-client.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "ariels",
    name  = "Ariel Shaqed (Scolnicov)",
    email = "ariels@treeverse.io",
    url   = url("https://github.com/arielshaqed")
  ),
  Developer(
    id    = "baraktr",
    name  = "B. A.",
    email = "barak.amar@treeverse.io",
    url   = url("https://github.com/nopcoder"),
  ),
  Developer(
    id    = "ozkatz",
    name  = "Oz Katz",
    email = "oz.katz@treeverse.io",
    url   = url("https://github.com/ozkatz"),
  ),
  Developer(
    id    = "johnnyaug",
    name  = "J. A.",
    email = "yoni.augarten@treeverse.io",
    url   = url("https://github.com/johnnyaug"),
  ),
)

credentials ++= Seq(
  Credentials(Path.userHome / ".sbt" / "credentials"),
  Credentials(Path.userHome / ".sbt" / "sonatype_credentials"),
)

ThisBuild / versionScheme := Some("early-semver")
ThisBuild / organization := "io.treeverse"
ThisBuild / organizationName := "Treeverse Labs"
ThisBuild / organizationHomepage := Some(url("http://treeverse.io"))
ThisBuild / description := "Spark client for lakeFS object metadata."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/treeverse/spark-client"))
