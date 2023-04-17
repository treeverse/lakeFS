---
layout: default
title: Committed Objects
description: Clean up unnecessary objects using the garbage collection feature in lakeFS.
parent: Garbage Collection
grand_parent: How-To
nav_order: 10
has_children: false
redirect_from: ["../howto/garbage-collection.html", "../reference/garbage-collection.html", "../howto/garbage-collection-index.html"]
---

## Garbage Collection: committed objects
{: .no_toc }

By default, lakeFS keeps all your objects forever. This allows you to travel back in time to previous versions of your data.
However, sometimes you may want to hard-delete your objects - namely, delete them from the underlying storage. 
Reasons for this include cost-reduction and privacy policies.

Garbage collection rules in lakeFS define for how long to retain objects after they have been deleted.
lakeFS provides a Spark program to hard-delete objects whose retention period has ended according to the GC rules.

This program does not remove any commits: you will still be able to use commits containing hard-deleted objects,
but trying to read these objects from lakeFS will result in a `410 Gone` HTTP status.

At this point, lakeFS supports Garbage Collection only on S3 and Azure.  We have [concrete plans](https://github.com/treeverse/lakeFS/issues/3626) to extend the support to GCP.     
{: .note}

[lakeFS Cloud](https://lakefs.cloud) users enjoy a managed Garbage Collection service, and do not need to run this Spark program.
{: .note }

{% include toc.html %}

### Understanding Garbage Collection

For every branch, the GC job retains deleted objects for the number of days defined for the branch.
In the absence of a branch-specific rule, the default rule for the repository is used.
If an object is present in more than one branch ancestry, it's retained according to the rule with the largest number of days between those branches.
That is, it's hard-deleted only after the retention period has ended for all relevant branches.

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

In the above example, objects are retained for 14 days after deletion by default. However, if they are present in the branch `main`, they are retained for 21 days.
Objects present in the `dev` branch (but not in any other branch) are retained for 7 days after they are deleted.

### Configuring GC rules

#### Using lakectl
{: .no_toc }

Use the `lakectl` CLI to define the GC rules: 

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

lakectl gc set-config lakefs://example-repo -f example_repo_gc_rules.json 
```

#### From the lakeFS UI
{: .no_toc }

1. Navigate to the main page of your repository.
2. Go to _Settings_ -> _Retention_.
3. Click _Edit policy_ and paste your GC rule into the text box as a JSON.
4. Save your changes.

![GC Rules From UI]({{ site.baseurl }}/assets/img/gc_rules_from_ui.png)


### Running the GC job
 
To run the job, use the following `spark-submit` command (or using your preferred method of running Spark programs).
The job will hard-delete objects that were deleted and whose retention period has ended according to the GC rules.

<div class="tabs">
  <ul>
    <li><a href="#aws-option">On AWS (Spark 3.1.2 and higher)</a></li>
    <li><a href="#aws-301-option">On AWS (Spark 3.0.1)</a></li>
    <li><a href="#aws-247-option">On AWS (Spark 2.x)</a></li>
    <li><a href="#azure-option">On Azure</a></li>
  </ul>
  <div markdown="1" id="aws-option">
  ```bash
spark-submit --class io.treeverse.clients.GarbageCollector \
  --packages org.apache.hadoop:hadoop-aws:2.7.7 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
  -c spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-312-hadoop3/0.6.5/lakefs-spark-client-312-hadoop3-assembly-0.6.5.jar \
  example-repo us-east-1
  ```
  </div>
  <div markdown="1" id="aws-301-option">
  ```bash
spark-submit --class io.treeverse.clients.GarbageCollector \
  --packages org.apache.hadoop:hadoop-aws:2.7.7 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
  -c spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-301/0.6.5/lakefs-spark-client-301-assembly-0.6.5.jar \
  example-repo us-east-1
  ```
  </div>
  <div markdown="1" id="aws-247-option">
  ```bash
spark-submit --class io.treeverse.clients.GarbageCollector \
  --packages org.apache.hadoop:hadoop-aws:2.7.7 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
  -c spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-247/0.6.5/lakefs-spark-client-247-assembly-0.6.5.jar \
  example-repo us-east-1
  ```
  </div>

  <div markdown="1" id="azure-option">

   If you want to access your storage using the account key:

  ```bash
spark-submit --class io.treeverse.clients.GarbageCollector \
  --packages org.apache.hadoop:hadoop-aws:3.2.1 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.azure.account.key.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=<AZURE_STORAGE_ACCESS_KEY> \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-312-hadoop3/0.6.5/lakefs-spark-client-312-hadoop3-assembly-0.6.5.jar \
  example-repo
  ```

   Or, if you want to access your storage using an Azure service principal:

  ```bash
spark-submit --class io.treeverse.clients.GarbageCollector \
  --packages org.apache.hadoop:hadoop-aws:3.2.1 \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.azure.account.auth.type.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=OAuth \
  -c spark.hadoop.fs.azure.account.oauth.provider.type.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider \
  -c spark.hadoop.fs.azure.account.oauth2.client.id.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=<application-id> \
  -c spark.hadoop.fs.azure.account.oauth2.client.secret.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=<service-credential-key> \
  -c spark.hadoop.fs.azure.account.oauth2.client.endpoint.<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net=https://login.microsoftonline.com/<directory-id>/oauth2/token \
  http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-312-hadoop3/0.6.5/lakefs-spark-client-312-hadoop3-assembly-0.6.5.jar \
  example-repo
  ```

**Notes:**
* On Azure, GC was tested only on Spark 3.3.0, but may work with other Spark and Hadoop versions.
* In case you don't have `hadoop-azure` package as part of your environment, you should add the package to your spark-submit with `--packages org.apache.hadoop:hadoop-azure:3.2.1`
* For GC to work on Azure blob, [soft delete](https://docs.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview) should be disabled.
</div>
</div>

You will find the list of objects hard-deleted by the job in the storage
namespace of the repository. It is saved in Parquet format under `_lakefs/logs/gc/deleted_objects`.

#### GC job options
{: .no_toc }

By default, GC first creates a list of expired objects according to your retention rules and then hard-deletes those objects. 
However, you can use GC options to break the GC job down into two stages: 
1. Mark stage: GC will mark the expired objects to hard-delete, **without** deleting them. 
2. Sweep stage: GC will hard-delete objects marked by a previous mark-only GC run. 

By breaking GC into these stages, you can pause and create a backup of the objects that GC is about to sweep and later 
restore them. You can use the [GC backup and restore](#backup-and-restore) utility to do that.   

###### Mark only mode 
{: .no_toc }

To make GC run the mark stage only, add the following properties to your spark-submit command:
```properties
spark.hadoop.lakefs.gc.do_sweep=false
spark.hadoop.lakefs.gc.mark_id=<MARK_ID> # Replace <MARK_ID> with your own identification string. This MARK_ID will enable you to start a sweep (actual deletion) run later
```
Running in mark only mode, GC will write the addresses of the expired objects to delete to the following location: `STORAGE_NAMESPACE/_lakefs/retention/gc/addresses/mark_id=<MARK_ID>/` as a parquet.

**Notes:** 
* Mark only mode is only available from v0.4.0 of lakeFS Spark client.
* The `spark.hadoop.lakefs.debug.gc.no_delete` property has been deprecated with v0.4.0.

###### Sweep only mode
{: .no_toc }

To make GC run the sweep stage only, add the following properties to your spark-submit command:
```properties
spark.hadoop.lakefs.gc.do_mark=false
spark.hadoop.lakefs.gc.mark_id=<MARK_ID> # Replace <MARK_ID> with the identifier you used on a previous mark-only run
```
Running in sweep only mode, GC will hard-delete the expired objects marked by a mark-only run and listed in: `STORAGE_NAMESPACE/_lakefs/retention/gc/addresses/mark_id=<MARK_ID>/`.

**Note:** Mark only mode is only available from v0.4.0 of lakeFS Spark client.

### Considerations

1. In order for an object to be hard-deleted, it must be deleted from all branches.
   You should remove stale branches to prevent them from retaining old objects.
   For example, consider a branch that has been merged to `main` and has become stale.
   An object which is later deleted from `main` will always be present in the stale branch, preventing it from being hard-deleted.

1. lakeFS will never delete objects outside your repository's storage namespace.
   In particular, objects that were imported using `lakectl ingest` or `UI Import Wizard` will not be affected by GC jobs.

1. In cases where deleted objects are brought back to life while a GC job is running, said objects may or may not be
   deleted. Such actions include:
   1. Reverting a commit in which a file was deleted.
   1. Branching out from an old commit.
   1. Expanding the retention period of a branch.
   1. Creating a branch from an existing branch, where the new branch has a longer retention period.

### Backup and restore 

GC was created to hard-delete objects from your underlying objects store according to your retention rules. However, when you start
using the feature you may want to first gain confidence in the decisions GC makes. The GC backup and restore utility helps you do that. 

**Use-cases:**
* Backup: copy expired objects from your repository's storage namespace to an external location before running GC in [sweep only mode](#sweep-only-mode).  
* Restore: copy objects that were hard-deleted by GC from an external location you used for saving your backup into your repository's storage namespace.

Follow [rclone documentation](https://rclone.org/docs/) to configure remote access to the underlying storage used by lakeFS.
Replace `LAKEFS_STORAGE_NAMESPACE` with remote:bucket/path which points to the lakeFS repository storage namespace.
The `BACKUP_STORAGE_LOCATION` attribute points to a storage location outside your lakeFS storage namespace into which you want to save the backup.

#### Backup command
{: .no_toc }

```shell
rclone --include "*.txt" cat "<LAKEFS_STORAGE_NAMESPACE>/_lakefs/retention/gc/addresses.text/mark_id=<MARK_ID>/" | \
  rclone -P --no-traverse --files-from - copy <LAKEFS_STORAGE_NAMESPACE> <BACKUP_STORAGE_LOCATION>
```

#### Restore command
{: .no_toc }

```shell
rclone --include "*.txt" cat "<LAKEFS_STORAGE_NAMESPACE>/_lakefs/retention/gc/addresses.text/mark_id=<MARK_ID>/" | \
  rclone -P --no-traverse --files-from - copy <BACKUP_STORAGE_LOCATION> <LAKEFS_STORAGE_NAMESPACE>
```

#### Example
{: .no_toc }

The following of commands used to backup/resource a configured remote 'azure' (Azure blob storage) to access example repository storange namespace `https://lakefs.blob.core.windows.net/repo/example/`:

```shell
# Backup
rclone --include "*.txt" cat "azure://repo/example/_lakefs/retention/gc/addresses.text/mark_id=a64d1885-6202-431f-a0a3-8832e4a5865a/" | \
  rclone -P --no-traverse --files-from - copy azure://repo/example/ azure://backup/repo-example/

# Restore
rclone --include "*.txt" cat "azure://tal/azure-br/_lakefs/retention/gc/addresses.text/mark_id=a64d1885-6202-431f-a0a3-8832e4a5865a/" | \
  rclone -P --no-traverse --files-from - copy azure://backup/repo-example/ azure://repo/example/
```
