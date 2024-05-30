---
title: Mount
description: This section covers the Everest feature for mounting a lakeFS path to your local filesystem.
grand_parent: Reference
parent: Features
---

# Mount (Everest)
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

lakeFS Enterprise
{: .label .label-purple }


Everest is a complementary binary to lakeFS that allows users to virtually mount a remote lakeFS repository onto a local directory.
Once mounted, users can access the data as if it resides on their local filesystem, using any tool, library, or framework that reads from a local filesystem.
This functionality is currently supported only on macOS using the NFS (Network File System) protocol.

There is no installation required. Users can download a binary from S3 upon request.

## Command Line Interface

### Mount Command
The `mount` command is used to mount a lakeFS repository to a local directory. 

#### Usage
```bash
everest mount <lakefs_uri> <mount_directory>

Flags
--presign: Use presign for downloading.
--cache-dir: Directory to cache read files in.
--cache-size: Size of the local cache in bytes.
--cache-create-provided-dir: If cache-dir is explicitly provided and does not exist, create it.
--listen: Address to listen on.
--no-spawn: Do not spawn a new server, assume one is already running.
--protocol: Protocol to use (default: nfs).
--log-level: Set logging level.
--log-format: Set logging output format.
--log-output: Set logging output(s).
umount Command
The umount command is used to unmount a currently mounted lakeFS repository.
```

### Umount Command
The `umount` command is used to unmount a currently mounted lakeFS repository.

```bash
everest umount <data_directory>
````

### mount-server Command
The mount-server command starts a mount server manually. Generally, users would use the mount command which handles server operations automatically.

```bash
everest mount-server <remote_mount_uri>
Flags
--cache-dir: Directory to cache read files and metadata.
--cache-create-provided-dir: Create the cache directory if it does not exist.
--listen: Address to listen on.
--protocol: Protocol to use (nfs | webdav).
--callback-addr: Callback address to report back to.
--log-level: Set logging level.
--log-format: Set logging output format.
--log-output: Set logging output(s).
--cache-size: Size of the local cache in bytes.
--parallelism: Number of parallel downloads for metadata.
--presign: Use presign for downloading.
```

## Examples

#### Data Exploration
Mount the lakeFS repository and explore data as if it's on the local filesystem.

```bash
everest mount "lakefs://image-repo/main/datasets/pets/" "./pets"
ls -l "./pets/dogs/"
find ./pets -name "*.small.jpg"
open -a Preview "./pets/dogs/golden_retrievers/cute.jpg"
everest umount "./pets"

```
#### Working with Data Locally
Mount the remote lakeFS server and use all familiar tools without changing the workflow.
```bash
everest mount --prefetch "lakefs://image-repo/main/datasets/pets/" "./pets"
pytorch_train.py --input "./pets"
duckdb "SELECT * FROM read_parquet('pets/labels.parquet')"
everest umount "./pets"
```

