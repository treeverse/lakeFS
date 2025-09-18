// Dependencies for the *build* (meta-build), not for the runtime JAR
libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"      % "2.25.39",
  "software.amazon.awssdk" % "regions" % "2.25.39"
)
