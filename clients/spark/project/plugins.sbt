addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.19.0")
addSbtPlugin("cf.janga" % "sbts3" % "0.10.5")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.29")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")
addDependencyTreePlugin

// ===== Build-time deps for code inside build.sbt (safeS3Upload) =====
val awsv2 = "2.25.63"
libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3" % awsv2,
  "software.amazon.awssdk" % "url-connection-client" % awsv2,
  "software.amazon.awssdk" % "aws-core" % awsv2
)
// ================================================================