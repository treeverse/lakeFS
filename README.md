# lakeFS

![Go](https://github.com/treeverse/lakeFS/workflows/Go/badge.svg)

This is a draft for a design document describing the capabilities and 
implementation of lakeFS 0.1

## Goals

lakeFS is data lake management solution, offering at a high level the following capabilities:

1. Cross-lake ACID operations - change several objects/collections as one atomic operations to avoid inconsistencies during complex migrations/recalculations
2. Reproducibility - Travel backwards in time and match versions of data to the code that generated it
3. Deduping by default - No more copying input/sample data to side directories that are later a nightmare to manage and track (see #2)
4. Collaboration - allow teams to share data and approve changes to data including review and validation steps
5. Production Safety - Accidentally deleted/overwritten/corrupted a critical collection? revert instantly.
6. Format agnostic - use Parquet, image files, csv's, all of the above. It doesn't matter. Works with structured or unstructured data

## How?

To achieve this, we require 4 main capabilities:

1. Git-like semantics that can scale to many petabytes of data (or terabytes of metadata)
   1. Committing and rolling back versions
   2. [Snapshot Isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) in such that one branch's changes are completely isolated from other branches
   3. Branching and merging is (relatively) cheap to perform and should be done often
   4. transaction support (i.e. throw-away branch that gets merged on commit and discarded on rollback)
2. Sit between processing and data: this will allow us to observe who/how data is being read and written
   1. Strongly consistent writes and reads, including list-after-write and read-after-write (removing the need for S3Guard/EMRFS/etc.)
3. API compatibility with common Object Stores, starting with the S3 API (see API subset bellow)
4. Metadata journaling to allow view materialization, corruption recovery and allowing migration out of the service

## Non-Goals

1. Improve performance/durability/availability
2. Compute/orchestration management. This should be a solution added to existing systems without migration costs or upfront investment
3. S3 API compatibility for anything other that what Spark/Hadoop/ML tooling is currently using (i.e. their versioning, S3 select, etc)

## High level Architecture

For the MVP the following architecture is proposed:

![Architecture](arch.png)

## S3 Gateway

The gateway service is meant to emulate the [API exposed by S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) (or at least a subset of it that is required for Big Data and ML systems). If we can get MR and Spark to work with this, we're good.

The following methods should be implemented:

1. Identity and authorization
    1. [SIGv2](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html)
    2. [SIGv4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
2. Bucket operations:
    1. [HEAD bucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html) (just 200 if exists and permissions allow access)
3. Object operations:
    1. [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)
    2. [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
    3. [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
        1. Support for caching headers, ETag
        2. Support for range requests
        3. *No* support for SSE
        4. *No* support for Select operations
    4. [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)
    5. [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
        1. Support multi-part uploads (see below)
        2. *No* support for storage classes
        3. *No* object level tagging
4. Object Listing:
    1. [ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)
    2. [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
    3. [Delimiter support](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_RequestSyntax) (for `"/"` only)
5. Multipart Uploads:
    1. [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)
    2. [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
    3. [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
    4. [ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)
    5. [Upload Part](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
    6. [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)
 


## Block Adapter (S3)

The block adapter service is a very simple store, adhering to the following interface:

```go
package block

import "io"

type Adapter interface {
	Put(repo string, identifier string, reader io.ReadSeeker) error
	Get(repo string, identifier string) (io.ReadCloser, error)
	GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error)
}
```

It uses a minimal surface area of S3 to achieve this to allow for maximum portability (in case we'll need to support bring-your-own-bucket or extend to other object stores easily)

## Indexing service

The indexing service holds the Merkle tree implementation and tracks operations to the repo. This is the brain.
This is the indexing interface (simplified):

### Data model

#### workspace KV (write buffer area, this is where files get updated/deleted in the critical path)

| Key                    | Value                     |
|------------------------|---------------------------|
| `(repo, branch, path)` | `(entry, tombstone)`      |


#### Merkle Tree Entry KV

| Key                         | Value      |
|-----------------------------|------------|
| `(repo, parent_hash, name)` | `entry`    |


#### Merkle Object KV 

| Key            | Value         |
|----------------|---------------|
| `(repo, hash)` | `(object)`    |

#### Merkle Commits KV

| Key            | Value                                  |
|----------------|----------------------------------------|
| `(repo, hash)` | `(metadata, root_tree, []parent_hash)` |

#### Branch KV

| Key                   | Value                              |
|-----------------------|------------------------------------|
| `(repo, branch_name)` | `(commit_hash, staging_root_hash)` |


#### Tree refcount index KV
| Key            | Value               |
|----------------|---------------------|
| `(repo, hash)` | `(reference_count)` |



### Operations

* **on read:**
    * get from workspace blob KV
    * if missing, traverse workspace_root
* **on write:**
    * write to workspace blob KV
    * if request is sampled (`rand() % N == 0`), do a partial commit (can be async)
* **on delete:**
    * do read logic to ensure object exists (either in workspace or tree)
    * if so, write a tombstone to KV
    * if request is sampled (`rand() % N == 0`), do a partial commit
* **on list:**
    * do partial commit
    * traverse workspace_root and enumerate children
* **on partial commit (see write/delete/list):**
    * range over changes in KV (and tombstones)
    * build new merkle tree
    * for every new tree, create an entry
    * incr tree ref count for every entry
    * replace branch KV's workspace_root
    * clear range
    * queue old workspace_root to GC loop
* **GC:**
    * read root's reference count*
    * decrement by 1. if it's 0, descend tree and compare child trees
    * delete all ref count = 0 nodes
* **on commit:**
    * do a partial commit
    * update both workspace_root, create commit object, write its hash as commit_hash
* **on merge:**
    * get 3 root trees: source, destination, common ancestor
    * compare both trees: 3-way diff using common ancestor. Could happen across multiple transactions since they are immutable and won't change.
    * assemble new tree (TODO: source always wins conflicts?, LWW? configurable?)
    * in transaction: ensure dest branch hash still the same, otherwise, start over (optimistic concurrency)
    * in transaction: create commit object with both parents
    * write new commit hash to branch KV
    * Prohibit merging a branch that has workspace entries or that its workspace root differs from its commit root (i.e. dirty writes)
* **on delete branch:**
    * delete branch kv entry
    * clear range on workspace
    * gc for workspace root if it's different from commit root

#### Partial Commits

committing a large change set into the Merkle tree can be expensive as it requires scanning a large number of keys to build the new tree.
Looking at the common access patterns for data lakes, adjacent nodes are usually created together (i.e. many files in a few partitions).

We can use this fact to optimize commit time by amortizing the cost of building a tree across write operations.

Instead of building a tree once on commit, we can do it every N writes, caping the amount of dirty writes that haven't been committed to the new tree.
This turns the workspace to a sort of buffer, holding dirty writes before they enter the tree.

while this is an optimization, it is probably required for correctness when working with FoundationDB, as it restricts transactions to 5 seconds.
This could make scanning a large workspace with many dirty writes always fail, so partial commits are also a mitigation for that.


#### Index Journaling

All mutations to metadata tree (branch create/delete, commits, merges, resets and object writes/deletes) are written to a journal.
Every message will include the following fields

* `logical_timestamp` - representing the logical time the event took place. Events are strictly serializable for causal consistency (i.e. for any single object or operation, a larger value will indicate that this event took place after smaller timestamped events). We'll use the committed transaction ID from FoundationDB since it ensures that.
* `client_id` - The client whose repo was modified
* `repo_id` - the repo being modified
* `event_type` - mutation that took place (merge, write, etc).
* `event_payload` - data relevant for that specific event type. This will include e.g. affected path, object id, branch name, etc.

This data will synchronously be written to Kafka as part of a transaction. A transaction will fail if writing the event does not succeed. This ensures that any mutation to the metadata layer must be consistent with the transaction log.

A stream processor will read these events and write them in batches to the repo's bucket. The collection will be partitioned by logical_timestamp to allow for easy lifecycle management and query performance

As an optimization, we can do a nightly compaction, removing old entries that have been superseded by newer mutations.

This should allow us to do a repo reconciliation in the future, in case of corruption, FDB failure or critical bug, to ensure we never lose customer data.

In case of a general failure, if we only have the S3 bucket with the underlying blocks and journal, we should be able to completely reconstruct the metadata.
For a large 1.5b object repo, with 10 FDB servers each [supporting 55k writes/sec](https://apple.github.io/foundationdb/performance.html#throughput-per-core) we should be able to reconstruct the entire state of the repo by replaying the journal in about 45 minutes, assuming we properly saturate the FDB cluster and have no write skew.


*TODO: See if we can use [Kafka's Log Compaction](https://kafka.apache.org/documentation/#compaction) to reduce some of the complexity*


## Auth service

The auth model is simple RBAC implementation with API keys but supporting a very simple domain of controls:

* CreateRepo
* DeleteRepo
* ReadRepo
* WriteRepo

The following roles will be preconfigured:

* Admin (CreateRepo, DeleteRepo, ReadRepo(\*), WriteRepo(\*))
* Developer (ReadRepo(\*), WriteRepo(\*))
* Analyst (ReadRepo(\*))


## retention tasks ("lifecycle" management)

1. Data retention - user configured based on the following rules:
   1. occurrence in specific branches (i.e. never delete a file that is not marked as deleted in master)
   2. Last written/updated/read (i.e. delete anything I haven't accessed in 30 days)
   3. Dangling objects not belonging to any branch
   4. Dangling blocks not belonging to any object 


## UI Frontend service

Provide the following functionality:

1. Index exploration
   1. List existing branches along with their metadata
   2. See commit history for every branch
   3. "Live" feed of events happening on any given branch
2. Collaboration
   1. "Pull requests" - request to merge a branch
   2. Commenting, approvals, merging from UI
3. Account administration
   1. repo/bucket management
      1. branch resolution paths
      2. IAM/Azure/GD connection
   2. Enterprise user management
      1. LDAP configuration
      2. SAML configuration
      3. Manual user definition
      4. RBAC management (users, groups, roles, permissions)
   3. Billing
4. Documentation
5. Support


## Action Flow
* Generic API request
    * Gateway: extract authentication credentials and context
    * Auth: resolve permissions and raise error if not allowed
* Writing a file
  * Gateway: Resolve branch and path for the request
  * Gateway: chunk into (64Mb?) blocks, pass along with hash to store
  * Store: Persist chunk with its hash
  * Index: WorkspaceWrite & journal
  * Gateway: async write to history
  * History: persist event
* Deleting a file
  * Gateway: Resolve branch and path for the request
  * Index: WorkspaceWrite a tombstone & journal
  * Gateway: async write to history
  * History: persist event
* Reading a file
  * Gateway: Resolve branch and path for the request
  * Index: Read(branch, path), reading from workspace or tree
  * Gateway: Request Object blocks from Store and stream them to client, along with Metadata stored for the object
  * Gateway: async write to history
  * History: persist event
* Listing by prefix
    * Gateway: Resolve branch and path for the request
    * Index: List(prefix)
* Committing
  * Gateway: Resolve branch
  * Index: Commit(branch) & journal
* Merging
  * Gateway: Resolve branches
  * Index: Merge(source branch, destination branch) & journal
* Deleting branch
  * Gateway: Resolve branch
  * Index: Delete(branch) & journal


## Running a lakefs server

#### Configuration

when running the lakefs binary, you can pass a yaml configuration file:

```shell script
$ lakefs --config /path/to/configuration.yaml
``` 

Here's an example configuration file:

```yaml
---
logging:
  format: text # or json
  level: DEBUG # or INFO, WARN, ERROR, NONE
  output: "-" # for stdout, or a path to a log file

metadata:
  db:
    type: badger # currently the only supported DB is an embedded badger using a local directory
    badger:
      path: "~/lakefs/metadata" 

blockstore:
  type: s3 # or "local"
  s3:
    region: us-east-1 
    profile: default # optional, implies using a credentials file
    credentials_file: /path/to/.aws/credentials # optional, will use the default AWS path if not specified
    credentials: # optional, will use these hard coded credentials if supplied
      access_key_id: "AKIA..."
      access_secret_key: "..."
      session_token: "..."
  
  # if instead of S3 you'd like to write the data itself locally
  local:
    path: ~/lakefs/data

gateways:
  s3:
    listen_address: "0.0.0.0:8000"
    domain_name: s3.example.com
    region: us-east-1

api:
  listen_address: "0.0.0.0:8001"

```

Here's a list of all possible values used in the configuration:

| Key                                           | Type                                                | Default Value         | Description                                                                                                                               |
|-----------------------------------------------|-----------------------------------------------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `logging.format`                              | one of `["json", "text"]`                           | `"text"`              | how to format the logfile                                                                                                                 |
| `logging.level`                               | one of `["DEBUG", "INFO", "WARN", "ERROR", "NONE"]` | `"DEBUG"`             | minimal log level to output                                                                                                               |
| `logging.output`                              | string                                              | `"-"`                 | where to write the log to (`"-"` meaning stdout. Otherwise will be treated as file name                                                   |
| `metadata.db.type`                            | string                                              | `"badger"`            | metadata DB type. Currently only `"badger"` is supported, implying [badgerDB](https://github.com/dgraph-io/badger)                        |
| `metadata.badger.path`                        | string                                              | `"~/lakefs/metadata"` | Where to store badgerDB's data files                                                                                                      |
| `blockstore.type`                             | one of `["local", "s3"]`                            | `"local"`             | Where to store the actual data files written to the system                                                                                |
| `blockstore.local.path`                       | string                                              | `" ~/lakefs/data"`    | Directory to store data written to the system when using the local blockstore type                                                        |
| `blockstore.s3.region`                        | string                                              | `"us-east-1"`         | Region used when writing to Amazon S3                                                                                                     | 
| `blockstore.s3.profile`                       | string                                              | N/A                   | If specified, will be used as a [named credentials profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) |
| `blockstore.s3.credentials_file`              | string                                              | N/A                   | If specified, will be used as a [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)             | 
| `blockstore.s3.credentials.access_key_id`     | string                                              | N/A                   | If specified, will be used as a static set of credential                                                                                  | 
| `blockstore.s3.credentials.access_secret_key` | string                                              | N/A                   | If specified, will be used as a static set of credential                                                                                  | 
| `blockstore.s3.credentials.session_token`     | string                                              | N/A                   | If specified, will be used as a static session token                                                                                      |
| `gateways.s3.listen_address`                  | string                                              | `"0.0.0.0:8000"`      | a `<host>:<port>` structured string representing the address to listen on                                                                 | 
| `gateways.s3.domain_name`                     | string                                              | `"s3.local"`          | a FQDN representing the S3 endpoint used by S3 clients to call this server                                                                | 
| `gateways.s3.region`                          | string                                              | `"us-east-1"`         | AWS region we're pretending to be. Should match the region configuration used in AWS SDK clients                                          |
| `api.listen_address`                          | string                                              | `"0.0.0.0:8001"`      |  a `<host>:<port>` structured string representing the address to listen on                                                                |
