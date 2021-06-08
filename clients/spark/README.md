# lakeFS Spark Metadata Client

Read metadata from lakeFS into Spark.

## Features

1. Read Graveler meta-ranges, ranges and entries.
1. Export data from lakeFS to any object storage (see [docs](https://docs.lakefs.io/reference/export.html)).

## Scala/Spark compatibility

Two versions are available for the client, compatible with the following Spark/Scala versions:
1. Spark 2 / Scala 2.11
1. Spark 3 / Scala 2.12

## Publishing a new version

We publish the client to Sonatype, and an Uber-Jar of the client to S3.
The Uber-Jar should be used when running into conflicting dependencies on environments like EMR, Databricks, etc.
Otherwise, the client can be included using Maven coordinates.

1. Have the following files ready:

   1. `~/.sbt/sonatype_credentials`, with the content:
      ```
       realm=Sonatype Nexus Repository Manager
       host=s01.oss.sonatype.org
       user=<your sonatype username>
       password=<your sonatype password>
      ```

   1. `~/.sbt/credentials`, with the content:
      ```
      realm=Amazon S3
      host=treeverse-clients-us-east.s3.amazonaws.com
      user=<AWS access key>
      password=<AWS secret key>
      ```
1. Increment the version in the build.sbt file.

1. From the lakeFS project root, run:
   ```bash
   make publish-scala
   ```