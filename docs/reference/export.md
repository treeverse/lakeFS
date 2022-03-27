---
layout: default
title: Exporting Data
description: Use lakeFS Spark client to export lakeFS commit to the object store.
parent: Reference
nav_order: 5
has_children: false
---

# Exporting Data
{: .no_toc }
The export operation copies all data from a given lakeFS commit to
a designated object store location.

For instance, the contents `lakefs://example/main` might be exported on
`s3://company-bucket/example/latest`.  Clients entirely unaware of lakeFS could use that
base URL to access latest files on `main`.  Clients aware of lakeFS can continue to use
the lakeFS S3 endpoint to access repository files on `s3://example/main`, as well as
other versions and uncommitted versions.

Possible use-cases:
1. External consumers of data don't have access to your lakeFS installation.
1. Some data pipelines in the organization are not fully migrated to lakeFS.
1. You want to experiment with lakeFS as a side-by-side installation first.
1. Create copies of your data lake in other regions (taking into account read pricing).

{% include toc.html %}

## Exporting Data With Spark 

### Using spark-submit
You can use the export main in 3 different modes:

1. Export all objects from branch `example-branch` on `example-repo` repository to s3 location `s3://example-bucket/prefix/`:

   ```shell
   .... example-repo s3://example-bucket/prefix/ --branch=example-branch
   ```


1. Export all objects from a commit `c805e49bafb841a0875f49cd555b397340bbd9b8` on `example-repo` repository to s3 location `s3://example-bucket/prefix/`:

   ```shell
   .... example-repo s3://example-bucket/prefix/ --commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
   ```

1. Export only the diff between branch `example-branch` and commit `c805e49bafb841a0875f49cd555b397340bbd9b8`
   on `example-repo` repository to s3 location `s3://example-bucket/prefix/`:

   ```shell
   .... example-repo s3://example-bucket/prefix/ --branch=example-branch --prev_commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
   ```

The complete `spark-submit` command would look like:

```shell
spark-submit --conf spark.hadoop.lakefs.api.url=https://<LAKEFS_ENDPOINT>/api/v1 \
  --conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY_ID> \
  --conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_ACCESS_KEY> \
  --packages io.lakefs:lakefs-spark-client-301_2.12:0.1.6 \
  --class io.treeverse.clients.Main export-app example-repo s3://example-bucket/prefix \
  --branch=example-branch
```

The command assumes the Spark cluster has permissions to write to `s3://example-bucket/prefix`.
Otherwise, add `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key` with the proper credentials.
{: .note}

### Using custom code (notebook/spark)
Set up lakeFS Spark metadata client with the endpoint and credentials as instructed in the previous [page](./spark-client.md){: .button-clickable}.

The client exposes the `Exporter` object with 3 export options:

1. Export *all* objects at the HEAD of a given branch. Does not include
files that were added to that branch, but were not committed.

```scala
exportAllFromBranch(branch: String)
```

2. Export ALL objects from a commit:

```scala
exportAllFromCommit(commitID: String)
```

3. Export just the diff between a commit and the HEAD of a branch.
   This is the ideal option for continuous exports of a branch, as it will change only the files
   that have been changed since the previous commit.

   ```scala
   exportFrom(branch: String, prevCommitID: String)
   ```

## Success/Failure Indications

When the Spark export operation ends, an additional status file will be added to the root
object storage destination.
If all files were exported successfully the file path will be of form: `EXPORT_<commitID>_<ISO-8601-time-UTC>_SUCCESS`.
For failures: the form will be`EXPORT_<commitID>_<ISO-8601-time-UTC>_FAILURE`, and the file will include a log of the failed files operations.

## Export Rounds (Spark success files)
Some files should be exported before others, e.g. a Spark `_SUCCESS` file exported before other files under
the same prefix might send the wrong indication.

The export operation may contain several *rounds* within the same export.
A failing round will stop the export of all the files of the next `rounds`.

By default, lakeFS will use the `SparkFilter` and have 2 `rounds` for each export.
The first round will export any non Spark `_SUCCESS` files. Second round will export all Spark's `_SUCCESS` files.
You may override the default behaviour by passing a custom `filter` to the `Exporter`.  

## Example

1. First configure the `Exporter` instance:

   ```scala
   import io.treeverse.clients.{ApiClient, Exporter}
   import org.apache.spark.sql.SparkSession

   val endpoint = "http://<LAKEFS_ENDPOINT>/api/v1"
   val accessKey = "<LAKEFS_ACCESS_KEY_ID>"
   val secretKey = "<LAKEFS_SECRET_ACCESS_KEY>"

   val repo = "example-repo"

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

1. Now you can export all objects from `main` branch to `s3://company-bucket/example/latest`:

   ```scala
   val branch = "main"
   exporter.exportAllFromBranch(branch)
   ```

1. Assuming a previous successful export on commit `f3c450d8cd0e84ac67e7bc1c5dcde9bef82d8ba7`,
you can alternatively export just the difference between `main` branch and the commit:

   ```scala
   val branch = "main"
   val commit = "f3c450d8cd0e84ac67e7bc1c5dcde9bef82d8ba7"
   exporter.exportFrom(branch, commit)
   ```

## Exporting Data With Docker

This option is recommended if you don't have Spark at your tool-set.
It does not support distribution across machines, therefore may have a lower performance. 
Using this method, you can export data from lakeFS to S3 using 3 export options (in a similar way to the Spark export):

1. Export all objects from a branch `example-branch` on `example-repo` repository to s3 location `s3://destination-bucket/prefix/`:

   ```shell
   .... example-repo s3://destination-bucket/prefix/ --branch="example-branch"
   ```


1. Export all objects from a commit `c805e49bafb841a0875f49cd555b397340bbd9b8` on `example-repo` repository to s3 location `s3://destination-bucket/prefix/`:

   ```shell
   .... example-repo s3://destination-bucket/prefix/ --commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
   ```


1. Export only the diff between branch `example-branch` and commit `c805e49bafb841a0875f49cd555b397340bbd9b8`
   on `example-repo` repository to s3 location `s3://destination-bucket/prefix/`:

   ```shell
   .... example-repo s3://destination-bucket/prefix/ --branch="example-branch" --prev_commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
   ```

You will need to add the relevant environment variables.
The complete `docker run` command would look like:

```shell 
docker run \
-e LAKEFS_ACCESS_KEY=XXX -e LAKEFS_SECRET_KEY=YYY -e LAKEFS_ENDPOINT=https://<LAKEFS_ENDPOINT>/ \
-e S3_ACCESS_KEY=XXX -e S3_SECRET_KEY=YYY \
-it treeverse/lakefs-rclone-export:latest example-repo s3://destination-bucket/prefix/ --branch="example-branch"
``` 

**Note:** This feature uses [rclone](https://rclone.org/){: target="_blank" .button-clickable},
and specifically [rclone sync](https://rclone.org/commands/rclone_sync/){: target="_blank" .button-clickable}. This can change the destination path, therefore the s3 destination location must be designated to lakeFS export.
{: .note}