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
This functionality is currently in limited support and is a Read-Only file system and is pointing to a commit in lakeFS.

**Note**
⚠️ Everest is currently in beta. There is no installation required, please [contact us](mailto:support@treeverse.io) to get access to the Everest binary.
{: .note }

{% include toc.html %}

## Requirements

- For enterprise installations: lakeFS Version `1.25.0` or higher.
  
### OS and Protocol Support

Currently the implemented protocols are `nfs` and `fuse`. 
- NFS V3 (Network File System) is supported on macOS.
- FUSE is supported on Linux (no root required). 

## Authentication with lakeFS 

The authentication with the target lakeFS server is equal to [lakectl CLI][lakectl].
Searching for lakeFS credentials and server endpoint in the following order:
- Command line flags `--lakectl-access-key-id`, `--lakectl-secret-access-key` and `--lakectl-server-url`
- `LAKECTL_*` Environment variables
- `~/.lakectl.yaml` Configuration file or via `--lakectl-config` flag
  
## Command Line Interface

### Mount Command

The `mount` command is used to mount a lakeFS repository to a local directory, it does it in 2 steps: 
- Step 1: Starting a server that listens on a local address and serves the data from the remote lakeFS repository.
- Step 2:  Running the required mount command on the OS level to connect the server to the local directory.

#### Tips:

- Since the server runs in the background set `--log-output /some/file` to view the logs in a file.
- Cache: Everest uses a local cache to store the data and metadata of the lakeFS repository. The optimal cache size is the size of the data you are going to read.
- Reusing Cache: between restarts of the same mount endpoint, set `--cache-dir` to make sure the cache is reused.
- Mounted data consistency: When providing lakeFS URI mount endpoint `lakefs://<repo>/<ref>/<path>` the `<ref>` should be a specific commit ID. If a branch/tag is provided, Everest will use the HEAD commit instead.

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
```

### Umount Command
The `umount` command is used to unmount a currently mounted lakeFS repository.

```bash
everest umount <data_directory>
````

### mount-server Command (Advanced)

**Note**
⚠️ The `mount-server` command is for advanced use cases and will only spin the server without calling OS mount command.
{: .note }

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

**Note**
⚠️ For simplicity, the examples show `main` as the ref, Everest will always mount a specific commit ID, given a ref it will use the HEAD (e.g most recent commit).
{: .note }

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
everest mount "lakefs://image-repo/main/datasets/pets/" "./pets"
pytorch_train.py --input "./pets"
duckdb "SELECT * FROM read_parquet('pets/labels.parquet')"
everest umount "./pets"
```

[lakectl]: {% link reference/cli.md %}