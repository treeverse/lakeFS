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


### Operations

* **on read:**
    * get from workspace
    * if missing, traverse commit_root
* **on write:**
    * write to workspace
* **on delete:**
    * do read logic to ensure object exists (either in workspace or tree)
    * if so, write a tombstone to workspace
* **on list:**
    * traverse commit_root, union with workspace (if ref is branch) and enumerate children
* **on commit:**
    * range over changes in workspace (and tombstones)
    * build new merkle tree
    * for every new tree, create an entry
    * clear workspace entries
    * create commit object, update branch with its id and root
* **on merge:**
    * get 3 root trees: source, destination, common ancestor
    * compare both trees: 3-way diff using common ancestor. Could happen across multiple transactions since they are immutable and won't change.
    * assemble new tree
    * create commit object with both parents
    * write new commit hash to branch
    * Prohibit merging into branch that has workspace entries (i.e. dirty writes)
* **on delete branch:**
    * delete branch entries
    * clear range on workspace
    * gc for workspace root if it's different from commit root


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

* ManageRepo
* ReadRepo
* WriteRepo

The following roles will be preconfigured:

* Admin (ManageRepo(\*), ReadRepo(\*), WriteRepo(\*))


## UI Frontend service

Provide the following functionality:

1. Index exploration
   1. List existing branches along with their metadata
   2. See commit history for every branch
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
  * Gateway: pipe body into store using a UUID as identifier
  * Store: Persist stream with its identifier
  * Index: write to workspace using hash of content, dedupe if needed
* Deleting a file
  * Gateway: Resolve branch and path for the request
  * Index: write a tombstone to workspace
* Reading a file
  * Gateway: Resolve branch and path for the request
  * Index: Read(branch, path), reading from workspace or commit tree
  * Gateway: Request Object blocks from Store and stream them to client, along with Metadata stored for the object
* Listing by prefix
    * Gateway: Resolve branch and path for the request
    * Index: List(prefix)
* Committing
  * Gateway: Resolve branch
  * Index: Commit(branch) & journal
* Merging
  * Gateway: Resolve branches
  * Index: Merge(source branch, destination branch)
* Deleting branch
  * Gateway: Resolve branch
  * Index: Delete(branch)


## Running a lakefs server

#### Prerequisites:

1. A running PostgreSQL server, version 11+ (RDS and Aurora should work great). No need to create schemas
2. An S3 bucket for block storage, including an access_key_id/secret_access_key pair to be used by LakeFS
3. A linux server with at least 2GB of memory. We recommend using instances with high network throughput (10Gbps recommended). Optionally, LakeFS can easily run on ECS/Kubernetes/other container schedulers.
4. A server configuration file. See "Configuration" below

#### Before deployment
 
1. Apply PostgreSQL schema:
    
    ```shell script
    $ lakefs --config /path/to/configuration.yaml setupdb 
    ```
    
    This will create all the necessary database tables
2. Create a default admin user and credentials
    
    ```shell script
    $ lakefs --config /path/to/configuration.yaml init \
          --email 'my.name@example.com' \
          --full-name 'My Name' 
    ```
    
    An admin user will be created for you, along with an access_key_id and secret_access_key.
    Keep both of these safe - the secret key will only be displayed once and cannot be restored (you can always generate a new pair though).


#### Deployment

1. Run the server using the following command:

    ```shell script
    $ lakefs --config /path/to/configuration.yaml run
    ```

2. This command exposes 2 ports, as configured in the configuration bellow - S3 Gateway and API.
If you're using a load balancer such as ELB to direct traffic to your instance, you can configure the following health checks:

    1. For the S3 gateway, use `/_health` (guaranteed to respond with 200 OK without authentication)
    2. For the API server, use `/` (this is the UI's index page that will also respond with 200 OK)


3. For the S3 Gateway, you'll need to define the following DNS settings:

    1. The value of `gateways.s3.domain_name` (e.g. `s3.example.com`), should point to the load balancer or LakeFS instance
    2. A wildcard for all subdomains of the above (e.g. `*.s3.example.com`), pointing at the same load balancer or instance

4. *Optional but highly recommended:* Setup SSL for both `s3.example.com` and `*.s3.example.com`.
SSL termination should be done by a load balancer or reverse proxy. See [this](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/create-https-listener.html) as an example on how to set this up for AWS ELB.

5. *Also optional* - send logs to centralized logging: most frameworks allow reading from a file. The logging format could be changed to json by setting the `logging.format` configuration parameter to `"json"`.


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
    # Make sure the DB connection string includes search_path (no need to create this schema beforehand)
    uri: "postgres://localhost:5432/postgres?search_path=lakefs_index"

auth:
  db:
    # Make sure the DB connection string includes search_path (no need to create this schema beforehand)
    uri: "postgres://localhost:5432/postgres?search_path=lakefs_auth"
  encrypt:
    # This value must be set by the user.
    # In production it's recommended to read this value from a safe place like AWS KMS or Hashicorp Vault
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc09e90b6641393a0a89d1a645dcf990fbd5f48cae092a5eee7b804e45c7d6a20e6b840e8124334312e01dde9a087228485512feb0780f4589d01fd2cc825dbb1925c3968c95083c2fca5ac07d61a10d15fdb6f43236dc5347dddfa3e7852f1654410ef53082b0007f33387dcdfd735c5b48e61991ceef3e8bba7267af4f0383a73af07b0c767ddd78b9a771ccb8be3d6662191f1b76d0e725ac59f1a63d110b018c2d0a727097ed9363fcb3f822d8dc7f12584bda25182cd74fece779977ca24caf774a3d5e3579228b27bbac99a5b7384367a5a6f3da629d00159edec45bc8fa"


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
  
  # if instead of S3 you'd like to write the data itself locally (for testing only!)
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

| Key                                           | Type                                                | Default Value                                                   | Description                                                                                                                               |
|-----------------------------------------------|-----------------------------------------------------|-----------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `logging.format`                              | one of `["json", "text"]`                           | `"text"`                                                        | how to format the logfile                                                                                                                 |
| `logging.level`                               | one of `["DEBUG", "INFO", "WARN", "ERROR", "NONE"]` | `"DEBUG"`                                                       | minimal log level to output                                                                                                               |
| `logging.output`                              | string                                              | `"-"`                                                           | where to write the log to (`"-"` meaning stdout. Otherwise will be treated as file name                                                   |
| `metadata.db.uri`                             | string                                              | `"postgres://localhost:5432/postgres?search_path=lakefs_index"` | Valid PostgreSQL connection string that includes a search_path query parameter (schema name to use. Doesn't have to exist)                |
| `auth.db.uri`                                 | string                                              | `"postgres://localhost:5432/postgres?search_path=lakefs_auth"`  | Valid PostgreSQL connection string that includes a search_path query parameter (schema name to use. Doesn't have to exist)                |
| `auth.encrypt.secret_key`                     | string                                              | N/A                                                             | Required. This key is used as a symmetric encryption key to store sensitive data encrypted. Store in a KMS or somewhere safe!             |
| `blockstore.type`                             | one of `["local", "s3"]`                            | `"local"`                                                       | Where to store the actual data files written to the system                                                                                |
| `blockstore.local.path`                       | string                                              | `" ~/lakefs/data"`                                              | Directory to store data written to the system when using the local blockstore type                                                        |
| `blockstore.s3.region`                        | string                                              | `"us-east-1"`                                                   | Region used when writing to Amazon S3                                                                                                     | 
| `blockstore.s3.profile`                       | string                                              | N/A                                                             | If specified, will be used as a [named credentials profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) |
| `blockstore.s3.credentials_file`              | string                                              | N/A                                                             | If specified, will be used as a [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)             | 
| `blockstore.s3.credentials.access_key_id`     | string                                              | N/A                                                             | If specified, will be used as a static set of credential                                                                                  | 
| `blockstore.s3.credentials.access_secret_key` | string                                              | N/A                                                             | If specified, will be used as a static set of credential                                                                                  | 
| `blockstore.s3.credentials.session_token`     | string                                              | N/A                                                             | If specified, will be used as a static session token                                                                                      |
| `gateways.s3.listen_address`                  | string                                              | `"0.0.0.0:8000"`                                                | a `<host>:<port>` structured string representing the address to listen on                                                                 | 
| `gateways.s3.domain_name`                     | string                                              | `"s3.local"`                                                    | a FQDN representing the S3 endpoint used by S3 clients to call this server                                                                | 
| `gateways.s3.region`                          | string                                              | `"us-east-1"`                                                   | AWS region we're pretending to be. Should match the region configuration used in AWS SDK clients                                          |
| `api.listen_address`                          | string                                              | `"0.0.0.0:8001"`                                                |  a `<host>:<port>` structured string representing the address to listen on                                                                |

