name := "sonnets"

version := "0.1"

crossScalaVersions := Seq("2.12.13", "2.11.12")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
    "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.375" % "provided",
    "org.apache.hadoop" % "hadoop-aws" % "3.2.0" % "provided",
)
