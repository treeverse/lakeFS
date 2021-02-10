name := "lakefs-client"
scalaVersion := "2.12.10"

organization := "io.treeverse"
version := "1.0"

Compile / PB.protoSources := Seq(file("../catalog/"), file("../graveler/committed"))
Compile / PB.targets := Seq(
  scalapb.gen(flatPackage=true) -> (Compile / sourceManaged).value
)

libraryDependencies += "org.rocksdb" % "rocksdbjni" % "6.6.4"
libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
libraryDependencies += "commons-codec" % "commons-codec" % "1.15"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.946"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.http.**" -> "org.apache.httpShaded@1").inAll
)
