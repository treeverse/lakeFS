---
layout: default
title: Import data into lakeFS 
description: In order to import existing data to lakeFS, you may choose to copy it using S3 CLI or using tools like Apache DistCp.
parent: Setup lakeFS
nav_order: 20
has_children: false
redirect_from: ../reference/import.html
---
# Import data into lakeFS
{: .no_toc }

{% include toc.html %}

## Use external tools

In order to import existing data to lakeFS, you may choose to copy it using [S3 CLI](../integrations/aws_cli.md#copy-from-a-local-path-to-lakefs) 
or using tools like [Apache DistCp](../integrations/distcp.md#from-s3-to-lakefs). This is the most straightforward way, and we recommend it if it’s applicable for you.

## Zero-copy import

lakeFS supports 3 ways to ingest objects from the object store without copying the data.
They differ by the outcome, scale and ease of use:

1. Import from the UI - A UI dialog to trigger an import to a designated import branch. It creates a commit from all imported objects. Easy to use and scales well.
1. `lakectl ingest` - Use a simple CLI command to create uncommitted objects in a branch. It will make sequential calls between the CLI and the server, which make scaling difficult.
1. `lakefs import` - Use the lakeFS binary to create a commit from an S3 inventory file. Supported only with lakeFS with S3 storage adapters.
   Requires the most setup - lakeFS binary with DB access and an S3 inventory file. The most scalable option.


### UI Import

Clicking the Import button from any branch will open the following dialog:

![ui-import.png](../assets/img/UI-Import-Dialog.png)

If it's the first import to `my-branch`, it will create the import branch named `_my-branch_imported`.
lakeFS will import all objects from the Source URI to the import branch under the given prefix.

Hang tight! It might take several minutes for the operation to complete:

![img.png](../assets/img/ui-import-waiting.png)

Once the import is completed, you can merge the changes from the import branch to the source branch.

![img.png](../assets/img/ui-import-completed.png)

#### Prerequisite

lakeFS must have permissions to list the objects at the source object store,
and on the same region of your destination bucket.

#### Limitations

Import is feasible only from source object storage that matches the storage namespace of the
current repository.  lakeFS S3 installation cannot import from Azure bucket.

Although created by lakeFS, import branches are just like any other branch.
Authorization policies, CI/CD triggering, branch protection rules and all other lakeFS concepts apply to them
as they apply to any other branch.
{: .note }


### lakectl ingest

For cases where copying data is not feasible, the `lakectl` command supports ingesting objects from a source object store without actually copying the data itself.
This is done by listing the source bucket (and optional prefix), and creating pointers to the returned objects in lakeFS.

By doing this, it's possible to take even large sets of objects, and have them appear as objects in a lakeFS branch, as if they were written directly to it.

For this to work, make sure that:

1. The user calling `lakectl ingest` must have permissions to list the objects at the source object store
2. The lakeFS installation has read permissions to the objects being ingested.
3. The source path is **not** a storage namespace used by lakeFS. e.g. if `lakefs://my-repo` created with storage namespace `s3://my-bucket`, then `s3://my-bucket/*` cannot be an ingestion source.

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

The `lakectl ingest` command will attempt to use the current user's existing credentials and will respect instance profiles,
environment variables and credential files [in the same way that the AWS cli does](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html){: target="_blank" }
Specify an endpoint to ingest from other S3 compatible storage solutions, e.g. add `--s3-endpoint-url https://play.min.io`.
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

**Note:** Currently `lakectl import` supports the `http://` and `https://` schemes for Azure storage URIs. `wasb`, `abfs` or `adls` are currently not supported.
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


### lakefs import

Importing a very large amount of objects (> ~250M) might take some time using `lakectl ingest` as described above,
since it has to paginate through all the objects in the source using API calls.

For S3, we provide a utility as part of the `lakefs` binary, called `lakefs import`.

The lakeFS import tool will use the [S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html) feature to create lakeFS metadata.
The imported metadata will be committed to a special branch, called `import-from-inventory`.

You should not make any changes or commit anything to branch `import-from-inventory`: it will be operated on only by lakeFS.
After importing, you will be able to merge this branch into your main branch.
{: .note }

#### How it works
{: .no_toc }

The imported data is not copied to the repository’s dedicated bucket.
Rather, it will be read directly from your existing bucket when you access it through lakeFS.
Files created or replaced through lakeFS will then be stored in the repository’s dedicated bucket.

It is important to note that due to the deduplication feature of lakeFS, data will be read from your original bucket even
when accessing it through other branches. In a sense, your original bucket becomes an initial snapshot of your data.

**Note:** lakeFS will never make any changes to the import source bucket.
{: .note .pb-3 }

#### Prerequisites
{: .no_toc }

- Your bucket should have S3 Inventory enabled.
- The inventory should be in Parquet or ORC format.
- The inventory must contain (at least) the size, last-modified-at, and e-tag columns.
- The S3 credentials you provided to lakeFS should have GetObject permissions on the source bucket and on the bucket where the inventory is stored.
- If you want to use the tool for [gradual import](#gradual-import), you should not delete the data for the most recently imported inventory, until a more recent inventory is successfully imported.

For a step-by-step walkthrough of this process, see the post [3 Ways to Add Data to lakeFS](https://lakefs.io/3-ways-to-add-data-to-lakefs/) on our blog.
{: .note .note-info }

#### Usage
{: .no_toc }

Import is performed by the `lakefs import` command.

Assuming your manifest.json is at `s3://example-bucket/path/to/inventory/YYYY-MM-DDT00-00Z/manifest.json`, and your lakeFS configuration yaml is at `config.yaml` (see notes below), run the following command to start the import:

```bash
lakefs import lakefs://example-repo -m s3://example-bucket/path/to/inventory/YYYY-MM-DDT00-00Z/manifest.json --config config.yaml
```

You will see the progress of your import as it is performed.
After the import is finished, a summary will be printed along with suggestions for commands to access your data.

```
Added or changed objects: 565000
Deleted objects: 0
Commit ref: cf349ded0a0e65e20bd3b25ea8d9b656c2870b7f1f32f60eb1d90ca5873b6c03

Import to branch import-from-inventory finished successfully.
To list imported objects, run:
	$ lakectl fs ls lakefs://example-repo/cf349ded0a0e65e20bd3b25ea8d9b656c2870b7f1f32f60eb1d90ca5873b6c03/
To merge the changes to your main branch, run:
	$ lakectl merge lakefs://example-repo/import-from-inventory lakefs://goo/main
```

##### Merging imported data to the main branch
{: .no_toc }

As previously mentioned, the above command imports data to the dedicated `import-from-inventory` branch.
By adding the `--with-merge` flag to the import command, this branch will be automatically merged to your main branch immediately after the import.

```bash
lakefs import --with-merge lakefs://example-repo -m s3://example-bucket/path/to/inventory/YYYY-MM-DDT00-00Z/manifest.json --config config.yaml
```

##### Notes
{: .no_toc }

1. Perform the import from a machine with access to your database, and on the same region of your destination bucket.

1. You can download the `lakefs` binary from [here](https://github.com/treeverse/lakeFS/releases). Make sure you choose one compatible with your installation of lakeFS.

1. Use a configuration file like the one used to start your lakeFS installation. This will be used to access your database. An example can be found [here](../reference/configuration.html#example-aws-deployment).

**Warning:** the *import-from-inventory* branch should only be used by lakeFS. You should not make any operations on it.
{: .note } 

#### Gradual Import
{: .no_toc }

Once you switch to using the lakeFS S3-compatible endpoint in all places, you can stop making changes to your original bucket.
However, if your operation still requires that you work on the original bucket,
you can repeat using the import API with up-to-date inventories every day, until you complete the onboarding process.
You can specify only the prefixes that require import. lakeFS will merge those prefixes with the previous imported inventory.
For example, a prefixes-file that contains only the prefix `new/data/`. The new commit to `import-from-inventory` branch will include all objects from the HEAD of that branch, except for objects with prefix `new/data/` that is imported from the inventory. 

#### Limitations
{: .no_toc }

Note that lakeFS cannot manage your metadata if you make changes to data in the original bucket.
The following table describes the results of making changes in the original bucket, without importing it to lakeFS:

| Object action in the original bucket | ListObjects result in lakeFS                 | GetObject result in lakeFS |
|--------------------------------------|----------------------------------------------|----------------------------|
| Create                               | Object not visible                           | Object not accessible      |
| Overwrite                            | Object visible with outdated metadata        | Updated object accessible  |
| Delete                               | Object visible                               | Object not accessible      |


## Importing from public buckets

lakeFS needs access to the imported location to first list the files to import and later
to read the files upon users request. 

There are some use-cases where the user would like to import from a destination which isn't owned by the account running lakeFS.
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