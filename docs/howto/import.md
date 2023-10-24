---
title: Import Data
description: Import existing data into a lakeFS repository
parent: How-To
redirect_from: 
  - /setup/import.html
---

# Import data into lakeFS

The simplest way to bring data into lakeFS is by [copying it](#copying-data-into-a-lakefs-repository), but this approach may not be suitable when a lot of data is involved.
To avoid copying the data, lakeFS offers [Zero-copy import](#zero-copy-import). With this approach, lakeFS only creates pointers to your existing objects in your new repository.

{% include toc_2-3.html %}

## Zero-copy import

Mirror an existing object store location into a lakeFS repository, without copying the data.

### Prerequisites

* Importing is permitted for users in the Supers (lakeFS open-source) group or the SuperUsers (lakeFS Cloud/Enterprise) group.
   To learn how lakeFS Cloud and lakeFS Enterprise users can fine-tune import permissions, see [Fine-grained permissions](#fine-grained-permissions) below.
* The lakeFS _server_ must have permissions to list the objects in the source bucket.
* The source bucket must be in the same region as your repository.

### Using the lakeFS UI

1. In your repository's main page, click the _Import_ button to open the import dialog:

   ![lakeFS UI import dialog]({% link assets/img/UI-Import-Dialog.png %})

2. Under _Import from_, fill in the location on your object store you would like to import from.
3. Fill in the import destination in lakeFS 
4. Add a commit message, and optionally metadata.
5. Press _Import_

Once the import is complete, the changes are merged into the destination branch.

#### Notes
* Any previously existing objects under the destination prefix will be deleted.
* The import duration depends on the amount of imported objects, but will roughly be a few thousand objects per second.

### Using the CLI: _lakectl import_
The _lakectl import_ command acts the same as the UI import wizard. It commits the changes to a dedicated branch, with an optional
flag to merge the changes to a selected branch.

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

### Limitations

1. Importing is only possible from the object storage service in which your installation stores its data. For example, if lakeFS is configured to use S3, you cannot import data from Azure.
2. For security reasons, if you are using lakeFS on top of your local disk (`blockstore.type=local`), you need to enable the import feature explicitly. 
   To do so, set the `blockstore.local.import_enabled` to `true` and specify the allowed import paths in `blockstore.local.allowed_external_prefixes` (see [configuration reference]({% link reference/configuration.md %})).
   Since there are some differences between object-stores and file-systems in the way directories/prefixes are treated, local import is allowed only for directories. 
3. Making changes to data in the original bucket will not be reflected in lakeFS, and may cause inconsistencies. 

### Fine-grained permissions
{:.no_toc}
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }
lakeFS Enterprise
{: .label .label-purple }

With RBAC support, The lakeFS user running the import command should have the following permissions in lakeFS:
`fs:WriteObject`, `fs:CreateMetaRange`, `fs:CreateCommit`, `fs:ImportFromStorage` and `fs:ImportCancel`.

As mentioned above, all of these permissions are available by default to the Supers (lakeFS open-source) group or the SuperUsers (lakeFS Cloud/Enterprise).

### Provider-specific permissions
{:.no_toc}

In addition, the following for provider-specific permissions may be required:

<div class="tabs">
<ul>
  <li><a href="#aws-s3">AWS S3</a></li>
  <li><a href="#azure-storage">Azure Storage</a></li>
  <li><a href="#gcs">Google Cloud Storage</a></li>
</ul>
<div markdown="1" id="aws-s3">


#### AWS S3: Importing from public buckets

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

</div>
<div markdown="1" id="azure-storage">
See [Azure deployment][deploy-azure-storage-account-creds] on limitations when using account credentials.

#### Azure Data Lake Gen2

lakeFS requires a hint in the import source URL to understand that the provided storage account is ADLS Gen2

```
   For source account URL:
      https://<my-account>.core.windows.net/path/to/import/

   Please add the *adls* subdomain to the URL as follows:
      https://<my-account>.adls.core.windows.net/path/to/import/
```

</div>
<div markdown="1" id="gcs">
No specific prerequisites
</div>
</div>

## Copying data into a lakeFS repository

Another way of getting existing data into a lakeFS repository is by copying it. This has the advantage of having the objects along with their metadata managed by the lakeFS installation, along with lifecycle rules, immutability guarantees and consistent listing. However, do make sure to account for storage cost and time.

To copy data into lakeFS you can use the following tools:

1. The `lakectl` command line tool - see the [reference][lakectl-fs-upload] to learn more about using it to copy local data into lakeFS. Using `lakectl fs upload --recursive` you can upload multiple objects together from a given directory.
1. Using [rclone](./copying.md#using-rclone)
1. Using Hadoop's [DistCp](./copying.md#using-distcp)

[deploy-azure-storage-account-creds]:  {% link howto/deploy/azure.md %}#storage-account-credentials
[lakectl-fs-upload]:  {% link reference/cli.md %}#lakectl-fs-upload
