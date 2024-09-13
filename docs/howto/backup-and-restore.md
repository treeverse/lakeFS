---
title: Backup and Restore Repository
description: Use the lakeFS DumpRefs and RestoreRefs commands to backup and restore lakeFS repository
parent: How-To
---

# Backup and Restore Repository
This section explains how to backup and restore lakeFS repository for different use-cases:
1. Disaster Recovery: you want to backup the repository regularly so you can restore it in case of any disaster.  You'd also need to make sure to backup the repository's storage namespace to another, preferably geographically separate location.
1. Migrate Repository: you want to migrate a repository from one environment to another lakeFS environment.
1. Clone Repository: you want to clone a repository.

{% include toc.html %}

Refer to [Python Sample Notebooks](https://github.com/treeverse/lakeFS-samples/tree/main/01_standalone_examples/migrate-or-clone-repo) to backup, migrate or clone a lakeFS repository
{: .note}

## Commit Changes
Backup process doesn’t backup uncommitted data, so make sure to commit any staged writes before running the backup. But this is an optional process.

You can manually commit the changes by using lakeFS UI or you can programatically commit any uncommitted changes. This Python example shows how to programatically loop through all branches in lakeFS and commit any uncommitted data but this might take a lot of time if you have many branches in the repo:

```python
import lakefs

repo = lakefs.Repository("example-repo")

for branch in repo.branches():
    for diff in repo.branch(branch.id).uncommitted():
        repo.branch(branch.id).commit(message='Committed changes to backup the repository')
        break
```

## Backup Repository
### Dump Metadata
Dump metadata/refs of the repository by using lakeFS API or CLI.

* Example code to dump metadata by using lakeFS Python SDK (this process will create `_lakefs/refs_manifest.json` file in your storage namespace for the repository):

```python
lakefs_sdk_client.internal_api.dump_refs("example-repo")
```

* Example commands to dump metadata by using [lakeFS CLI](https://docs.lakefs.io/reference/cli.html#lakectl-refs-dump) and upload to S3 storage for the repository:

```
lakectl refs-dump lakefs://example-repo > refs_manifest.json

aws s3 cp refs_manifest.json s3://source-bucket-name/example-repo/_lakefs/refs_manifest.json
```

* Example commands to dump metadata by using [lakeFS CLI](https://docs.lakefs.io/reference/cli.html#lakectl-refs-dump) and upload to Azure Blob storage for the repository:

```
lakectl refs-dump lakefs://example-repo > refs_manifest.json

az storage blob upload --file refs_manifest.json --container-name sourceContainer --name example-repo/_lakefs/refs_manifest.json --account-name source-storage-account-name --account-key <source-storage-account-key>
```

Shutdown lakeFS services immediately after dumping the metadata so nobody can make any changes in the source repository.
{: .note}

### Copy Data to Backup Storage Location
Copy the repository’s storage namespace to another, preferably geographically separate location. Copy command depends on the type of object storage and the tool that you use.


* Example S3 command:

```
aws s3 sync s3://source-bucket-name/example-repo s3://target-bucket-name/example-repo
```

* Example Azure azcopy command:

```
azcopy copy 'https://source-storage-account-name.blob.core.windows.net/sourceContainer/example-repo/*?source_container_SAS_token' 'https://target-storage-account-name.blob.core.windows.net/targetContainer/example-repo?target_container_SAS_token' --recursive
```

You can restart lakeFS services after copying the data to backup storage location.
{: .note}


## Restore Repository
### Create a new Bare Repository
Create a bare lakeFS repository with a new name if you want to clone the repository or use the same repository name if you want to migrate or restore the repository.

* Python example to create a bare lakeFS repository using S3 storage:

```python
lakefs.Repository("target-example-repo").create(bare=True, storage_namespace="s3://target-bucket-name/example-repo", default_branch="same-default-branch-as-in-source-repo")
```

* Python example to create a bare lakeFS repository using Azure storage:

```python
lakefs.Repository("target-example-repo").create(bare=True, storage_namespace="https://target-storage-account-name.blob.core.windows.net/targetContainer/example-repo", default_branch="same-default-branch-as-in-source-repo")
```

* [lakeFS CLI](https://docs.lakefs.io/reference/cli.html#lakectl-repo-create-bare) command to create a bare lakeFS repository using S3 storage:

```
lakectl repo create-bare lakefs://target-example-repo s3://target-bucket-name/example-repo --default-branch "same-default-branch-as-in-source-repo"
```

* [lakeFS CLI](https://docs.lakefs.io/reference/cli.html#lakectl-repo-create-bare) command to create a bare lakeFS repository using Azure storage:

```
lakectl repo create-bare lakefs://target-example-repo https://target-storage-account-name.blob.core.windows.net/targetContainer/example-repo --default-branch "same-default-branch-as-in-source-repo"
```

### Restore Metadata to new Repository
Run restore_refs to load back all commits, tags and branches.

* Python example to restore metadata to new repository. First download metadata(refs_manifest.json) file created by metadata dump process:

```
aws s3 cp s3://target-bucket-name/example-repo/_lakefs/refs_manifest.json .
```

```
azcopy copy 'https://target-storage-account-name.blob.core.windows.net/targetContainer/example-repo/_lakefs/refs_manifest.json?<target_container_SAS_token>' .
```

Then read refs_manifest.json file and restore metadata to new repository:
```python
with open('./refs_manifest.json') as file:
    refs_manifest_json = json.load(file)
    print(refs_manifest_json)
    
target_lakefs_sdk_client.internal_api.restore_refs(target_repo_name, refs_manifest_json)
```

* [lakeFS CLI](https://docs.lakefs.io/reference/cli.html#lakectl-refs-restore) command to restore metadata to new repository using S3 storage:

```
aws s3 cp s3://target-bucket-name/example-repo/_lakefs/refs_manifest.json - | lakectl refs-restore lakefs://target-example-repo --manifest -
```

* [lakeFS CLI](https://docs.lakefs.io/reference/cli.html#lakectl-refs-restore) command to restore metadata to new repository using Azure storage:

```
az storage blob download --container-name targetContainer --name example-repo/_lakefs/refs_manifest.json --account-name target-storage-account-name --account-key <target-storage-account-key> | lakectl refs-restore lakefs://target-example-repo --manifest -
```

**Note:** If you are running backups regularly, it is highly advised to test the restore process periodically to make sure that you are able to restore the repository in case of disaster.
{: .note}
