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
⚠️ No installation is required. Please [contact us](http://info.lakefs.io/thanks-lakefs-mounts) to get access to the Everest binary.
{: .note }

{% include toc.html %}

<iframe width="420" height="315" src="https://www.youtube.com/embed/BgKuoa8LAaU"></iframe>

## Use Cases

* **Simplified Data Loading**: With lakeFS Mount, there's no need to write custom data loaders or use special SDKs. You can use your existing tools to read files directly from the filesystem.
* **Handle Large-scale Data Without changing Work Habits**: Seamlessly scale from a few local files to millions without changing your tools or workflow. Use the same code from early experimentation all the way to production.
* **Enhanced Data Loading Efficiency**: lakeFS Mount supports billions of files and offers fast data fetching, making it ideal for optimizing GPU utilization and other performance-sensitive tasks.

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

## FAQs

<!-- START EXCLUDE FROM TOC -->

### How do I get started with lakeFS Mount (Everest)?

lakeFS Mount is avaialble for lakeFS Cloud and lakeFS Enterprise customers. Once your setup is complete, [contact us](http://info.lakefs.io/thanks-lakefs-mounts) to access the lakeFS Mounts binary and follow the provided docs.

* Want to try lakeFS Cloud? [Signup](https://lakefs.cloud/register) for a 30-day free trial.
* Interested in lakeFS Enterprise? [Contact sales](https://lakefs.io/contact-sales/) for a 30-day free license.

###  Can I write to lakeFS using lakeFS Mount?

Currently, lakeFS Mount supports read-only file system operations. Write support is on our roadmap and will be added in the future.

### What operating systems are supported by lakeFS Mount?

lakeFS Mount supports Linux and MacOS. Windows support is on the roadmap.

### How can I control access to my data when using lakeFS Mount?

You can use lakeFS’s existing [Role-Based Access Control mechanism](../security/rbac.md), which includes repository and path-level policies. lakeFS Mount translates filesystem operations into lakeFS API operations and authorizes them based on these policies.

The minimal RBAC permissions required for mounting a prefix from a lakeFS repository looks like this:
```json
{
  "id": "MountPolicy",
  "statement": [
    {
      "action": [
        "fs:ReadObject"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repository-name>/object/<prefix>/*"
    },
    {
      "action": [
        "fs:ListObjects",
        "fs:ReadCommit",
        "fs:ReadRepository"

      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repository-name>"
    }
  ]
}
```

### Does data pass through the lakeFS server when using lakeFS Mount?

lakeFS Mount leverages  pre-signed URLs to read data directly from the underlying object store, meaning data doesn’t  pass through the lakeFS server. By default, presign is enabled. To disable it, use:
```shell
everest mount <lakefs_uri> <mount_directory> --presign=false
```

### What happens if a lakeFS branch is updated after I mount it?

lakeFS Mount points to the commit that was the HEAD commit of the branch at the time of mounting. This means the local directory reflects the branch state at the time of mounting and does not update with subsequent branch changes.

### When are files downloaded to my local environment?
lakeFS Mount uses a lazy prefetch strategy. Files are not downloaded at mount time or during operations that only inspect file metadata (e.g., `ls`). Files are downloaded only when commands that require file access (e.g., `cat`) are used.

### What are the scale limitations of lakeFS Mount, and what are the recommended configurations for dealing with large datasets?

When using lakeFS Mount, the volume of data accessed by the local machine influences the scale limitations more than the total size of the dataset under the mounted prefix. This is because lakeFS Mount uses a lazy downloading approach, meaning it only downloads the accessed files. lakeFS Mount listing capability is limited to performing efficiently for prefixes containing fewer than 8000 objects, but we are working to increase this limit.

##### Recommended Configuration

Ensure your **cache size** is large enough to accommodate the volume of files being accessed.

### How does lakeFS Mount integrate with a Git repository?

It is perfectly safe to mount a lakeFS path within a Git repository.
lakeFS Mount prevents git from adding mounted objects to the git repository (i.e when running `git add -A`) by adding a virtual `.gitignore` file to the mounted directory.


The .gitignore file will also instruct Git to ignore all files except `.everest/source` and in its absence, it will try to find a `.everest/source` file in the destination folder, and read the lakeFS URI from there.
Since `.everest/source` is in source control, it will mount the same lakeFS commit every time!

### I’m already using lakectl local for working with lakeFS data locally, why should I use lakeFS Mount?

While both lakectl local and lakeFS Mount enable working with lakeFS data locally, they serve different purposes:

##### Use lakectl local

* For enabling lakeFS writes with [lakectl local commit](https://docs.lakefs.io/reference/cli.html#lakectl-local-commit).
* To integrate seamlessly with [Git](https://docs.lakefs.io/integrations/git.html).

##### Use lakeFS Mount

For read-only local data access. lakeFS Mount offers several benefits over lakectl local:
* **Optimized selective data access**: The lazy prefetch strategy saves storage space and reduces latency by only fetching the required data.
* **Reduced initial latency**: Start working on your data immediately without waiting for downloads.

**Note**
Write support for lakeFS Mount is on our roadmap.
{: .note }

<!-- END EXCLUDE FROM TOC -->

