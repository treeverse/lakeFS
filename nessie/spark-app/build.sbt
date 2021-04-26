name := "sonnets"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.375" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.0" % "provided"
