---
title: Export Data
description: Use the lakeFS Spark client or RClone inside Docker to export a lakeFS commit to the object store.
parent: How-To
redirect_from: 
  - /reference/export.html
---

# Exporting Data
The export operation copies all data from a given lakeFS commit to
a designated object store location.

For instance, the contents `lakefs://example/main` might be exported on
`s3://company-bucket/example/latest`. Clients entirely unaware of lakeFS could use that
base URL to access latest files on `main`. Clients aware of lakeFS can continue to use
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
You can use the export main in three different modes:

1. Export all the objects from branch `example-branch` on `example-repo` repository to S3 location `s3://example-bucket/prefix/`:

   ```shell
   .... example-repo s3://example-bucket/prefix/ --branch=example-branch
   ```


1. Export all the objects from a commit `c805e49bafb841a0875f49cd555b397340bbd9b8` on `example-repo` repository to S3 location `s3://example-bucket/prefix/`:

   ```shell
   .... example-repo s3://example-bucket/prefix/ --commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
   ```

1. Export only the diff between branch `example-branch` and commit `c805e49bafb841a0875f49cd555b397340bbd9b8`
   on `example-repo` repository to S3 location `s3://example-bucket/prefix/`:

   ```shell
   .... example-repo s3://example-bucket/prefix/ --branch=example-branch --prev_commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
   ```

The complete `spark-submit` command would look as follows:

```shell
spark-submit --conf spark.hadoop.lakefs.api.url=https://<LAKEFS_ENDPOINT>/api/v1 \
  --conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY_ID> \
  --conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_ACCESS_KEY> \
  --packages io.lakefs:lakefs-spark-client_2.12:0.14.0 \
  --class io.treeverse.clients.Main export-app example-repo s3://example-bucket/prefix \
  --branch=example-branch
```

The command assumes that the Spark cluster has permissions to write to `s3://example-bucket/prefix`.
Otherwise, add `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key` with the proper credentials.
{: .note}

#### Networking

Spark export communicates with the lakeFS server.  Very large repositories
may require increasing a read timeout.  If you run into timeout errors
during communication from the Spark job to lakeFS consider increasing these
timeouts:

* Add `-c spark.hadoop.lakefs.api.read.timeout_seconds=TIMEOUT_IN_SECONDS`
  (default 10) to allow lakeFS more time to respond to requests.
* Add `-c
  spark.hadoop.lakefs.api.connection.timeout_seconds=TIMEOUT_IN_SECONDS`
  (default 10) to wait longer for lakeFS to accept connections.

### Using custom code (Notebook/Spark)

Set up lakeFS Spark metadata client with the endpoint and credentials as instructed in the previous [page]({% link reference/spark-client.md %}).

The client exposes the `Exporter` object with three export options:

<ol><li>
Export *all* the objects at the HEAD of a given branch. Does not include
files that were added to that branch but were not committed.

<div class="tabs">
  <ul>
  <li><a href="#export-head-scala">Scala</a></li>
  </ul>
  <div markdown="1" id="export-head-scala">
```scala
exportAllFromBranch(branch: String)
```
  </div>
</div>
</li>
<li>Export ALL objects from a commit:

<div class="tabs">
  <ul>
  <li><a href="#export-commit-scala">Scala</a></li>
  </ul>
  <div markdown="1" id="export-commit-scala">
```scala
exportAllFromCommit(commitID: String)
```
  </div>
</div>
</li>
<li>Export just the diff between a commit and the HEAD of a branch.

   This is an ideal option for continuous exports of a branch since it will change only the files
   that have been changed since the previous commit.

<div class="tabs">
  <ul>
  <li><a href="#export-diffs-scala">Scala</a></li>
  </ul>
  <div markdown="1" id="export-diffs-scala">
```scala
exportFrom(branch: String, prevCommitID: String)
```
  </div>
</div>
</li>
</ol>

## Success/Failure Indications

When the Spark export operation ends, an additional status file will be added to the root
object storage destination.
If all files were exported successfully, the file path will be of the form: `EXPORT_<commitID>_<ISO-8601-time-UTC>_SUCCESS`.
For failures: the form will be`EXPORT_<commitID>_<ISO-8601-time-UTC>_FAILURE`, and the file will include a log of the failed files operations.

## Export Rounds (Spark success files)
Some files should be exported before others, e.g., a Spark `_SUCCESS` file exported before other files under
the same prefix might send the wrong indication.

The export operation may contain several *rounds* within the same export.
A failing round will stop the export of all the files of the next rounds.

By default, lakeFS will use the `SparkFilter` and have 2 rounds for each export.
The first round will export any non-Spark `_SUCCESS` files. Second round will export all Spark's `_SUCCESS` files.
You may override the default behavior by passing a custom `filter` to the Exporter.  

## Example

<ol><li>First configure the `Exporter` instance:

<div class="tabs">
  <ul>
    <li><a href="#export-custom-setup-scala">Scala</a></li>
  </ul>
  <div markdown="1" id="export-custom-setup-scala">
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
  </div>
</div></li>
<li>Now you can export all objects from `main` branch to `s3://company-bucket/example/latest`:

<div class="tabs">
  <ul>
  <li><a href="#export-custom-branch-scala">Scala</a></li>
  </ul>
  <div markdown="1" id="export-custom-branch-scala">
```scala
val branch = "main"
exporter.exportAllFromBranch(branch)
```
  </div>
</div></li>
<li>Assuming a previous successful export on commit `f3c450d8cd0e84ac67e7bc1c5dcde9bef82d8ba7`,
you can alternatively export just the difference between `main` branch and the commit:

<div class="tabs">
  <ul>
    <li><a href="#export-custom-diffs-scala">Scala</a></li>
  </ul>
  <div markdown="1" id="export-custom-diffs-scala">
```scala
val branch = "main"
val commit = "f3c450d8cd0e84ac67e7bc1c5dcde9bef82d8ba7"
exporter.exportFrom(branch, commit)
```
  </div>
</div></li></ol>

## Exporting Data with Docker

This option is recommended if you don't have Spark at your tool-set.
It doesn't support distribution across machines, therefore may have a lower performance. 
Using this method, you can export data from lakeFS to S3 using the export options (in a similar way to the Spark export):

1. Export all objects from a branch `example-branch` on `example-repo` repository to S3 location `s3://destination-bucket/prefix/`:

   ```shell
   .... example-repo s3://destination-bucket/prefix/ --branch="example-branch"
   ```


1. Export all objects from a commit `c805e49bafb841a0875f49cd555b397340bbd9b8` on `example-repo` repository to S3 location `s3://destination-bucket/prefix/`:

   ```shell
   .... example-repo s3://destination-bucket/prefix/ --commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
   ```


1. Export only the diff between branch `example-branch` and commit `c805e49bafb841a0875f49cd555b397340bbd9b8`
   on `example-repo` repository to S3 location `s3://destination-bucket/prefix/`:

   ```shell
   .... example-repo s3://destination-bucket/prefix/ --branch="example-branch" --prev_commit_id=c805e49bafb841a0875f49cd555b397340bbd9b8
   ```

You will need to add the relevant environment variables.
The complete `docker run` command would look like:

```shell
docker run \
    -e LAKEFS_ACCESS_KEY_ID=XXX -e LAKEFS_SECRET_ACCESS_KEY=YYY \
   -e LAKEFS_ENDPOINT=https://<LAKEFS_ENDPOINT>/ \
   -e AWS_ACCESS_KEY_ID=XXX -e AWS_SECRET_ACCESS_KEY=YYY \
   treeverse/lakefs-rclone-export:latest \
      example-repo \
      s3://destination-bucket/prefix/ \
      --branch="example-branch"
```

**Note:** This feature uses [rclone](https://rclone.org/){: target="_blank"},
and specifically [rclone sync](https://rclone.org/commands/rclone_sync/){: target="_blank"}. This can change the destination path, therefore the s3 destination location must be designated to lakeFS export.
{: .note}
