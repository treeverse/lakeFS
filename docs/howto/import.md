---
layout: default
title: Import data into lakeFS 
description: Import existing data into a lakeFS repository
parent: How-To
nav_order: 10
has_children: false
redirect_from: 
  - ../setup/import.html
---

# Import data into lakeFS
{: .no_toc }

{% include toc.html %}

## Zero-copy import

### Importing using the lakeFS UI

#### Prerequisites

lakeFS must have permissions to list the objects at the source object store,
and in the same region of your destination bucket.

lakeFS supports two ways to ingest objects from the object store without copying the data:

1. [Importing using the lakeFS UI](#importing-using-the-lakefs-ui) - A UI dialog to trigger an import to a designated import branch. It creates a commit from all imported objects.
1. [Importing using lakectl cli](#importing-using-lakectl-cli) - You can use the [`lakectl` CLI command](../reference/cli.md#lakectl) to create uncommitted objects in a branch. It will make sequential calls between the CLI and the server.

#### Using the import wizard

Clicking the Import button from any branch will open the following dialog:

![Import dialog example configured with S3](../assets/img/UI-Import-Dialog.png)

If it's the first import to the selected branch, it will create the import branch named `_<branch_name>_imported`.
lakeFS will import all objects from the Source URI to the import branch under the given prefix.

The UI will update periodically with the amount of objects imported. How long it takes depends on the amount of objects to be imported but will roughly be a few thousand objects per second.

![img.png](../assets/img/ui-import-waiting.png)

Once the import is completed, you can merge the changes from the import branch to the source branch.

![img.png](../assets/img/ui-import-completed.png)


### Importing using lakectl cli

The `lakectl` cli supports _import_ and _ingest_ commands to import objects from an external source.

- The _import_ command acts the same as the UI import wizard. It imports (zero copy) and commits the changes on `_<branch_name>_imported` branch with an optional flag to also merge the changes to `<branch_name>`.
- The _Ingest_ is listing the source bucket (and optional prefix) from the client, and creating pointers to the returned objects in lakeFS. The objects will be staged on the branch.


#### Using the `lakectl import` command

##### Usage

<div class="tabs">
<ul>
  <li><a href="#import-tabs-1">AWS S3 or S3 API Compatible storage</a></li>
  <li><a href="#import-tabs-2">Azure Blob</a></li>
  <li><a href="#import-tabs-3">Google Cloud Storage</a></li>
</ul>
<div markdown="1" id="import-tabs-1">
```shell
lakectl import \
  --from s3://bucket/optional/prefix/ \
  --to lakefs://my-repo/my-branch/optional/path/
```
</div>
<div markdown="1" id="import-tabs-2">
```shell
lakectl import \
   --from https://storageAccountName.blob.core.windows.net/container/optional/prefix/ \
   --to lakefs://my-repo/my-branch/optional/path/
```
</div>
<div markdown="1" id="import-tabs-3">
```shell
lakectl import \
   --from gs://bucket/optional/prefix/ \
   --to lakefs://my-repo/my-branch/optional/path/
```
</div>
</div>

The imported objects will be committed to `_my-branch_imported` branch. If the branch does not exist, it will be created. The flag `--merge` will merge the branch `_my-branch_imported` to  `my-branch` after a successful import.


#### Using the `lakectl ingest` command

##### Prerequisites

1. The user calling `lakectl ingest` has permissions to list the objects at the source object store.
2. _Recommended_: The lakeFS installation has read permissions to the objects being ingested (to support downloading them directly from the lakeFS server)
3. The source path is **not** a storage namespace used by lakeFS. For example, if `lakefs://my-repo` created with storage namespace `s3://my-bucket`, then `s3://my-bucket/*` cannot be an ingestion source.

##### Usage

<div class="tabs">
<ul>
  <li><a href="#ingest-tabs-1">AWS S3 or S3 API Compatible storage</a></li>
  <li><a href="#ingest-tabs-2">Azure Blob</a></li>
  <li><a href="#ingest-tabs-3">Google Cloud Storage</a></li>
</ul>
<div markdown="1" id="ingest-tabs-1">
```shell
lakectl ingest \
  --from s3://bucket/optional/prefix/ \
  --to lakefs://my-repo/ingest-branch/optional/path/
```

The `lakectl ingest` command will attempt to use the current user's existing credentials and respect instance profiles,
environment variables, and credential files ([similarly to AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html){: target="_blank" })
Specify an endpoint to ingest from other S3 compatible storage solutions, e.g., add `--s3-endpoint-url https://play.min.io`.
</div>
<div markdown="1" id="ingest-tabs-2">
```shell
export AZURE_STORAGE_ACCOUNT="storageAccountName"
export AZURE_STORAGE_ACCESS_KEY="EXAMPLEroozoo2gaec9fooTieWah6Oshai5Sheofievohthapob0aidee5Shaekahw7loo1aishoonuuquahr3=="
lakectl ingest \
   --from https://storageAccountName.blob.core.windows.net/container/optional/prefix/ \
   --to lakefs://my-repo/ingest-branch/optional/path/
```

The `lakectl ingest` command currently supports storage accounts configured through environment variables as shown above.

**Note:** Currently, `lakectl import` supports the `http://` and `https://` schemes for Azure storage URIs. `wasb`, `abfs` or `adls` are currently not supported.
{: .note }
</div>
<div markdown="1" id="ingest-tabs-3">
```shell
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.gcs_credentials.json"  # Optional, will fallback to the default configured credentials
lakectl ingest \
   --from gs://bucket/optional/prefix/ \
   --to lakefs://my-repo/ingest-branch/optional/path/
```

The `lakectl ingest` command currently supports the standard `GOOGLE_APPLICATION_CREDENTIALS` environment variable [as described in Google Cloud's documentation](https://cloud.google.com/docs/authentication/getting-started).
</div>
</div>


### Limitations to importing data


Importing is only possible from the object storage service in which your installation stores its data. For example, if lakeFS is configured to use S3, you cannot import data from Azure.

Import is available for S3, GCP, Azure and the local storage adapters.
While the first 3 don't require special configurations, for security reasons you need to enable it for the local storage adapter by setting `blockstore.local.import_enabled` and specifying the allowed import paths `blockstore.local.allowed_external_prefixes` described in the [configuration](../reference/configuration.md).
Since there are some differences between object-stores and file-systems in the way directories/prefixes are treated, local import is allowed only for directories.

Although created by lakeFS, import branches behave like any other branch:
Authorization policies, CI/CD triggering, branch protection rules and all other lakeFS concepts apply to them as well.
{: .note }


### Working with imported data

Note that lakeFS cannot manage your metadata if you make changes to data in the original bucket.
The following table describes the results of making changes in the original bucket, without importing it to lakeFS:

| Object action in the original bucket | ListObjects result in lakeFS                 | GetObject result in lakeFS |
|--------------------------------------|----------------------------------------------|----------------------------|
| Create                               | Object not visible                           | Object not accessible      |
| Overwrite                            | Object visible with outdated metadata        | Updated object accessible  |
| Delete                               | Object visible                               | Object not accessible      |


### AWS S3: Importing from public buckets

lakeFS needs access to the imported location to first list the files to import and later read the files upon users request. 

There are some use cases where the user would like to import from a destination which isn't owned by the account running lakeFS.
For example, importing public datasets to experiment with lakeFS and Spark.

lakeFS will require additional permissions to read from public buckets. For example, for S3 public buckets,
the following policy needs to be attached to the lakeFS S3 service-account to allow access to public buckets, while blocking access to other owned buckets:

  ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "PubliclyAccessibleBuckets",
         "Effect": "Allow",
         "Action": [
            "s3:GetBucketVersioning",
            "s3:ListBucket",
            "s3:GetBucketLocation",
            "s3:ListBucketMultipartUploads",
            "s3:ListBucketVersions",
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts"
         ],
         "Resource": ["*"],
         "Condition": {
           "StringNotEquals": {
             "s3:ResourceAccount": "<YourAccountID>"
           }
         }
       }
     ]
   }
   ```

## Copying data into a lakeFS repository

Another way of getting existing data into a lakeFS repository is by copying it. This has the advantage of having the objects along with their metadata managed by the lakeFS installation, along with lifecycle rules, immutability guarantees and consistent listing. However, do make sure to account for storage cost and time.

To copy data into lakeFS you can use the following tools:

1. The `lakectl` command line tool - see the [reference](../reference/cli#lakectl-fs-upload) to learn more about using it to copy local data into lakeFS. Using `lakectl fs upload --recursive` you can upload multiple objects together from a given directory.
1. Using [rclone](../howto/copying.md#using-rclone)
1. Using Hadoop's [DistCp](../howto/copying.md#using-distcp)
