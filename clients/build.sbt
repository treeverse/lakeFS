name := "lakefs-client"
scalaVersion := "2.12.10"

organization := "io.treeverse"
version := "1.0"

Compile / PB.protoSources := Seq(file("../catalog/"), file("../graveler/committed"))
Compile / PB.targets := Seq(
  PB.gens.java -> (Compile / sourceManaged).value
)

libraryDependencies += "org.rocksdb" % "rocksdbjni" % "6.6.4"
libraryDependencies += "commons-codec" % "commons-codec" % "1.15"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.946"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies +=  "com.google.protobuf" % "protobuf-java" % "3.14.0" % "protobuf"

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.http.**" -> "org.apache.httpShaded@1").inAll
)
