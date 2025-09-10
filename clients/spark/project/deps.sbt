// Build-time dependencies only
val awsv2 = "2.25.63"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3" % awsv2,
  "software.amazon.awssdk" % "url-connection-client" % awsv2
)
