---
layout: default
title: Garbage Collection
description: Clean up expired objects using the garbage collection feature in lakeFS.
parent: Garbage Collection
grand_parent: How-To
nav_order: 1
has_children: false
redirect_from:
  - /reference/garbage-collection.html
  - /howto/garbage-collection-index.html
---

[lakeFS Cloud](https://lakefs.cloud) users enjoy a managed garbage collection service, and do not need to run this Spark program.
{: .tip }

# Garbage Collection

By default, lakeFS keeps all your objects forever. This allows you to travel back in time to previous versions of your data.
However, sometimes you may want to remove the objects from the underlying storage completely.
Reasons for this include cost-reduction and privacy policies.

The garbage collection job is a Spark program that removes the following from the underlying storage:
1. _Committed objects_ that have been deleted (or replaced) in lakeFS, and are considered expired according to [rules you define](#understanding-garbage-collection-rules).
2. _Uncommitted objects_ that are no longer accessible
   * For example, objects deleted before ever being committed.

{% include toc.html %}

## Understanding garbage collection rules

{: .note }
These rules only apply to objects that have been _committed_ at some point.
Without retention rules, only inaccessible _uncommitted_ objects will be removed by the job.

Garbage collection rules determine for how long an object is kept in the storage after it is _deleted_ (or replaced) in lakeFS.
For every branch, the GC job retains deleted objects for the number of days defined for the branch.
In the absence of a branch-specific rule, the default rule for the repository is used.
If an object is present in more than one branch ancestry, it is removed only after the retention period has ended for
all relevant branches.

Example GC rules for a repository:
```json
{
  "default_retention_days": 14,
  "branches": [
    {"branch_id": "main", "retention_days": 21},
    {"branch_id": "dev", "retention_days": 7}
  ]
}
```

In the above example, objects will be retained for 14 days after deletion by default.
However, if present in the branch `main`, objects will be retained for 21 days.
Objects present _only_ in the `dev` branch will be retained for 7 days after they are deleted.

## Configuring garbage collection rules

To define retention rules, either use the `lakectl` command or the lakeFS web UI:

<div class="tabs">
  <ul>
    <li><a href="#lakectl-option">CLI</a></li>
    <li><a href="#ui-option">Web UI</a></li>
  </ul>
  <div markdown="1" id="lakectl-option">

Create a JSON file with your GC rules:

```bash
cat <<EOT >> example_repo_gc_rules.json
{
  "default_retention_days": 14,
  "branches": [
    {"branch_id": "main", "retention_days": 21},
    {"branch_id": "dev", "retention_days": 7}
  ]
}
EOT
```

Set the GC rules using `lakectl`:
```bash
lakectl gc set-config lakefs://example-repo -f example_repo_gc_rules.json 
```

</div>
<div markdown="1" id="ui-option">
From the lakeFS web UI:

1. Navigate to the main page of your repository.
2. Go to _Settings_ -> _Garbage Collection_.
3. Click _Edit policy_ and paste your GC rule into the text box as a JSON.
4. Save your changes.

![GC Rules From UI]({{ site.baseurl }}/assets/img/gc_rules_from_ui.png)
</div>
</div>

## Running the GC job

To run the job, use the following `spark-submit` command (or using your preferred method of running Spark programs).

<div class="tabs">
  <ul>
    <li><a href="#aws-option">On AWS (Spark 3.1.2 and higher)</a></li>
    <li><a href="#aws-301-option">On AWS (Spark 3.0.1)</a></li>
    <li><a href="#azure-option">On Azure</a></li>
    <li><a href="#gcp-option">On GCP</a></li>
  </ul>
  <div markdown="1" id="aws-option">
  ```bash
spark-submit --class io.treeverse.gc.GarbageCollection \
  --packages org.apache.hadoop:hadoop-aws:2.7.7 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
  -c spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-312-hadoop3/0.8.1/lakefs-spark-client-312-hadoop3-assembly-0.8.1.jar \
  example-repo us-east-1
  ```
  </div>
  <div markdown="1" id="aws-301-option">
  ```bash
spark-submit --class io.treeverse.gc.GarbageCollection \
  --packages org.apache.hadoop:hadoop-aws:2.7.7 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
  -c spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-301/0.8.1/lakefs-spark-client-301-assembly-0.8.1.jar \
  example-repo us-east-1
  ```
  </div>

  <div markdown="1" id="azure-option">

If you want to access your storage using the account key:

  ```bash
spark-submit --class io.treeverse.gc.GarbageCollection \
  --packages org.apache.hadoop:hadoop-aws:3.2.1 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.azure.account.key.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=<AZURE_STORAGE_ACCESS_KEY> \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-312-hadoop3/0.8.1/lakefs-spark-client-312-hadoop3-assembly-0.8.1.jar \
  example-repo
  ```

Or, if you want to access your storage using an Azure service principal:

  ```bash
spark-submit --class io.treeverse.gc.GarbageCollection \
  --packages org.apache.hadoop:hadoop-aws:3.2.1 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.azure.account.auth.type.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=OAuth \
  -c spark.hadoop.fs.azure.account.oauth.provider.type.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider \
  -c spark.hadoop.fs.azure.account.oauth2.client.id.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=<application-id> \
  -c spark.hadoop.fs.azure.account.oauth2.client.secret.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=<service-credential-key> \
  -c spark.hadoop.fs.azure.account.oauth2.client.endpoint.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=https://login.microsoftonline.com/<directory-id>/oauth2/token \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-312-hadoop3/0.8.1/lakefs-spark-client-312-hadoop3-assembly-0.8.1.jar \
  example-repo
  ```

**Notes:**
* On Azure, GC was tested only on Spark 3.3.0, but may work with other Spark and Hadoop versions.
* In case you don't have `hadoop-azure` package as part of your environment, you should add the package to your spark-submit with `--packages org.apache.hadoop:hadoop-azure:3.2.1`
* For GC to work on Azure blob, [soft delete](https://docs.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview) should be disabled.
</div>

<div markdown="1" id="gcp-option">
⚠️ At the moment, only the "mark" phase of the Garbage Collection is supported for GCP.
That is, this program will output a list of expired objects, and you will have to delete them manually.
We have [concrete plans](https://github.com/treeverse/lakeFS/issues/3626) to extend this support to actually delete the objects.
{: .note .note-warning }

```bash
spark-submit --class  io.treeverse.gc.GarbageCollection \
  --jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.google.cloud.auth.service.account.enable=true \
  -c spark.hadoop.google.cloud.auth.service.account.json.keyfile=<PATH_TO_JSON_KEYFILE> \
  -c spark.hadoop.fs.gs.project.id=<GCP_PROJECT_ID> \
  -c spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  -c spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  -c spark.hadoop.lakefs.gc.do_sweep=false  \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-312-hadoop3/0.8.1/lakefs-spark-client-312-hadoop3-assembly-0.8.1.jar \
  example-repo
```

This program will not delete anything.
Instead, it will find all the objects that are safe to delete and save a list containing all their keys, in Parquet format.
The list will then be found under the path:
```
gs://<STORAGE_NAMESPACE>/_lakefs/retention/gc/unified/<RUN_ID>/deleted/
```

Note that this is a path in your Google Storage bucket, and not in your lakeFS repository. 
It is now safe to remove the objects that appear in this list directly from the storage.

</div>
</div>

You will find the list of objects removed by the job in the storage
namespace of the repository. It is saved in Parquet format under `_lakefs/retention/gc/unified/<RUN_ID>/deleted/`.

### Mark and Sweep stages

You can break the job into two stages:
* _Mark_: find objects to remove, without actually removing them.
* _Sweep_: remove the objects.

#### Mark-only mode

To make GC run the mark stage only, add the following to your spark-submit command:
```properties
spark.hadoop.lakefs.gc.do_sweep=false
```

In mark-only mode, GC will write the keys of the expired objects under: `<REPOSITORY_STORAGE_NAMESPACE>/_lakefs/retention/gc/unified/<MARK_ID>/`.
_MARK_ID_ is generated by the job. You can find it in the driver's output:

```
Report for mark_id=gmc6523jatlleurvdm30 path=s3a://example-bucket/_lakefs/retention/gc/unified/gmc6523jatlleurvdm30
```

#### Sweep-only mode

To make GC run the sweep stage only, add the following properties to your spark-submit command:
```properties
spark.hadoop.lakefs.gc.do_mark=false
spark.hadoop.lakefs.gc.mark_id=<MARK_ID> # Replace <MARK_ID> with the identifier you obtained from a previous mark-only run
```

## Considerations

1. In order for an object to be removed, it must not exist on the HEAD of any branch.
   You should remove stale branches to prevent them from retaining old objects.
   For example, consider a branch that has been merged to `main` and has become stale.
   An object which is later deleted from `main` will always be present in the stale branch, preventing it from being removed.

1. lakeFS will never delete objects outside your repository's storage namespace.
   In particular, objects that were imported using `lakectl import` or the UI import wizard will not be affected by GC jobs.

1. In cases where deleted objects are brought back to life while a GC job is running (for example, by reverting a commit), 
   the objects may or may not be deleted.

1. Garbage collection does not remove any commits: you will still be able to use commits containing removed objects,
   but trying to read these objects from lakeFS will result in a `410 Gone` HTTP status.
