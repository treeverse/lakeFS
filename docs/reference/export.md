---
layout: default
title: Exporting data
description: Use lakeFS spark client to export lakeFS commit to the object store. 
parent: Reference
nav_order: 15
has_children: false
---

# Exporting data
{: .no_toc }
The export operation copies all data from a given lakeFS commit to 
a designated object store location.

For instance, the contents `lakefs://example@master` might be stored on
`s3://company-bucket/example/latest`.  Clients entirely unaware of lakeFS could use that
base URL to access latest files on `master`.  Clients aware of lakeFS can continue to use
the lakeFS S3 endpoint to access repository files on `s3://example/master`, as well as
other versions and uncommitted versions.

Possible use-cases:
1. External consumers of data don't have access to your lakeFS installation.
1. Some data pipelines in the organization are not fully migrated to lakeFS.
1. You want to experiment with lakeFS as a side-by-side installation first.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## How to use

Set up lakeFS spark metadata client with the endpoint and credentials as instructed in the previous [page](./spark-client.md).

The client exposes the `exporter` object with 3 export options:

1. Exporting ALL objects at the HEAD of a given branch. Does not include
uncommitted files which were added to that branch, but were not committed.
   
```scala
exportAllFromBranch(branch: String)
```

2. Exporting ALL objects from a commit:

```scala
exportAllFromCommit(commitID: String)
```

3. Exporting just the diff between a commit and the HEAD of a branch.
This is the ideal option for continuous exports of a branch, as it will change only the files
that were changed from the previous commit.

```scala
exportFrom(branch: String, prevCommitID: String)
```   

## Success/Failure Indications
When the Spark export operation ends, an additional status file will be added to the root 
object storage location.
If all files were exported successfully the file path will be of form: `EXPORT_<commitID>_SUCCESS`.
and for failures: `EXPORT_<commitID>_FAILURE`, and the file will include a log of the failed files operations.

## Export Rounds (spark success files)
Some files should be exported before others, e.g. a Spark `_SUCCESS` file exported before other files under
the same prefix might send the wrong indication.

The export operation may contain several `rounds` within the same export.
A failing round will stop the export of all the files of the next `rounds`.

By default, lakeFS will use the `SparkFilter` and have 2 `rounds` for each export.
The first round will export any non Spark `_SUCCESS` files. Second round will export all Spark's `_SUCCESS` files.
Users may override the default behaviour by passing a custom `filter` to the `exporter`.  

## Example

1. First configure the `exporter` instance:

 ```scala
   import io.treeverse.clients.{ApiClient, Exporter}
   import org.apache.spark.sql.SparkSession

   val endpoint = "http://<LAKEFS_ENDPOINT>/api/v1"
   val accessKey = "<LAKEFS_ACCESS_KEY_ID>"
   val secretKey = "<LAKEFS_SECRET_ACCESS_KEY>"
   
   val repo = "my-repo"

   val spark = SparkSession.builder().appName("I can export").master("local").getOrCreate()
   val sc = spark.sparkContext
   sc.hadoopConfiguration.set("lakefs.api.url", endpoint)
   sc.hadoopConfiguration.set("lakefs.api.access_key", accessKey)
   sc.hadoopConfiguration.set("lakefs.api.secret_key", secretKey)
   
   // Add any required spark context configuration for s3 
   val rootLocation = "s3://company-bucket/example/latest"
   
   val apiClient = new ApiClient(endpoint, accessKey, secretKey)
   val exporter = new Exporter(spark, apiClient, repo, rootLocation)
 
 ```

a. Now you can export all objects from `main` branch under `s3://company-bucket/example/latest`:

```scala
val branch = "main"
exporter.exportAllFromBranch(branch)
```

b. Assuming a previous successfull export on commit `f3c450d8cd0e84ac67e7bc1c5dcde9bef82d8ba7`,
you can alternatively export just the difference between `main` branch and the commit:

```scala
val branch = "main"
val commit = "f3c450d8cd0e84ac67e7bc1c5dcde9bef82d8ba7"
exporter.exportFrom(branch, commit)
```

