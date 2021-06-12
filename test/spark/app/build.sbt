name := "sonnets"

version := "0.1"

val scala211 = "2.11.12"
val scala212 = "2.12.13"

ThisBuild / scalaVersion := scala212

ThisBuild / crossScalaVersions := Seq(scala212, scala211)

libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, scalaMajor)) if scalaMajor == 12 =>
            Seq(
                    "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
                    "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.375",
                    "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
               )
        case Some((2, scalaMajor)) if scalaMajor == 11 =>
            Seq(
                    "org.apache.spark" %% "spark-sql" % "2.4.6" % "provided",
                    "com.amazonaws" % "aws-java-sdk" % "1.7.4",
                    "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
               )
        case _ => Nil
    }
}

