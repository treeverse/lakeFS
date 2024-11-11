---
title: Performance Best Practices
parent: Understanding lakeFS
description: This section suggests performance best practices to work with lakeFS.
--- 
# Performance Best Practices

{% include toc.html %}

## Overview
Use this guide to achieve the best performance with lakeFS.

## Avoid concurrent commits/merges
Just like in Git, branch history is composed by commits and is linear by nature. 
Concurrent commits/merges on the same branch result in a race. The first operation will finish successfully while the rest will retry.

## Perform meaningful commits
It's a good idea to perform commits that are meaningful in the senese that they represent a logical point in your data's lifecycle. While lakeFS supports arbirartily large commits, avoiding commits with a huge number of objects will result in a more comprehensible commit history.

### Use zero-copy import
Zero-copy import is useful in case you have data already in the object store and you want to make lakeFS aware of it without copying data.

To import object into lakeFS, either a single time or regularly, lakeFS offers a [zero-copy import][zero-copy-import] feature.
Use this feature to import a large number of objects to lakeFS, instead of simply copying them into your repository.
This feature will create a reference to the existing objects on your bucket and avoids the copy.

Once you imported data to lakeFS, the source data should be considered frozen and immutable. Any changes to an imported object in the origin location 
will result in read failures when reading that object from lakeFS. You can re-import a dataset, and capture any changed or newly added files. 
Using regular imports is recommended for append-only setups rather than updated or overwritten files.

[Read more](https://lakefs.io/blog/import-data-lakefs/) on the different import patterns and how to use lakeFS in each case.    

## Read data using the commit ID
In cases where you are only interested in reading committed data: 
* Use a commit ID (or a tag ID) in your path (e.g: `lakefs://repo/a1b2c3`).
* Add `@` before the path  `lakefs://repo/main@/path`.

When accessing data using the branch name (e.g. `lakefs://repo/main/path`) lakeFS will also try to fetch uncommitted data, which may result in reduced performance.
For more information, see [how uncommitted data is managed in lakeFS][representing-refs-and-uncommitted-metadata].

## Operate directly on the storage
Storage operations can become a bottleneck when operating on large datasets. In such cases, it can be beneficial to perform
only versioning operations on lakeFS, while performing storage reads/writes directly on the object store. 

lakeFS offers multiple ways to do that: 

### Writing directly to the object store
In addition to importing large, batch-generated datasets, we may want to to add a few new files after an initial import, 
or to modify existing files. lakeFS allows “uploading” changes to a dataset. Unlike an import, in the case of an upload, 
lakeFS is in control of the actual location the file is stored at in the backing object store. This will not modify any 
directories you may have “imported” the dataset from originally, and you will need to use lakeFS to get consistent views
of the data. 

If we need to upload lots of files, we likely want to avoid writing directly through the lakeFS service. lakeFS allows this
in the following manner: we request a physical location from lakeFS to which a new file should be written, and then use 
natvie object store clients to upload the data directly.

To write data directly to the object storage, use one of the following methods:
* `lakectl fs upload --pre-sign` (Docs: [lakectl-upload][lakectl-upload]). The equivalent OpenAPI endpoint
will return a URL to which the user can upload the file(s) in question; other clients, such as the Java client, Python SDK,
etc, also expose presign functionality - consult relevant client documentation for details.
* The lakeFS [Hadoop Filesystem][hadoopfs].
* The [staging API][api-staging] which can be used to add lakeFS references to objects after having written them to the storage.

### Read directly from the object store
lakeFS maintains versions by keeping track of each file; the commit and branch paths (`my-repo/commits/{commit_id}`, `my-repo/branches/main`) 
are virtual and resolved by the lakefs service to iterate through appropriate files. 

lakeFS does allow you do either read directly from the lakefs API (or S3 gateways), or to query lakeFS API for underlying 
actual locations of the files that constitute a particular commit.

To read data from lakeFS without it being transferred through lakeFS:
* Read an object getObject (lakeFS OpenAPI) and add `--presign`. You'll get a link to download an object.
* Use statObject (lakeFS OpenAPI) which will return `physical_address` that is the actual S3/GCS path that you can read with any S3/GCS client.
* Use the `lakectl fs presign` ([docs][lakectl-fs-presign])
* Use the lakeFS [Hadoop Filesystem][hadoopfs]

#### Reading directly from GCS when using GCS-Fuse
GCP users commonly mount GCS buckets using `gcs-fuse`, particularly when using GCP's Vertex AI pipelines. The lakeFS Fuse integration is written in a way that ensures reads are scaleable and work directly off GCS, without involving any lakeFS services in the read path. This is achieved by using hooks to automatically create symlinks to reflect the virtual directory structure of branches and commits. See the [gcs-fuse integration][gcs-fuse] documentation for details. 

The end result is that you can read the branches or commits directly from the file system, and not involve lakeFS in the read path at all:

```
with open('/gcs/my-bucket/exports/my-repo/branches/main/datasets/images/001.jpg') as f:
    image_data = f.read()
```

```
commit_id = 'abcdef123deadbeef567'
with open(f'/gcs/my-bucket/exports/my-repo/commits/{commit_id}/datasets/images/001.jpg') as f:
    image_data = f.read()
```

## Zero-copy
lakeFS provides a zero-copy mechanism to data. Instead of copying the data, we can check out to a new branch. 
Creating a new branch will take constant time as the new branch points to the same data as its parent.
It will also lower the storage cost.


[hadoopfs]:  {% link integrations/spark.md %}#lakefs-hadoop-filesystem
[zero-copy-import]:  {% link howto/import.md %}#zero-copy-import
[lakectl-upload]:  {% link reference/cli.md %}#lakectl-fs-upload
[lakectl-download]:  {% link reference/cli.md %}#lakectl-fs-download
[lakectl-fs-presign]: {% link reference/cli.html %}#lakectl-fs-presign
[api-staging]:  {% link reference/api.md %}#operations-objects-stageObject
[representing-refs-and-uncommitted-metadata]:  {% link understand/how/versioning-internals.md %}#representing-references-and-uncommitted-metadata
[gcs-fuse]: {% link integrations/vertex_ai.md %}#using-lakefs-with-cloud-storage-fuse
