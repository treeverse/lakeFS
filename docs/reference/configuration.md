---
layout: default
title: Configuration Reference
description: Configuring lakeFS is done using a yaml configuration file. This reference uses . to denote the nesting of values.
parent: Reference
nav_order: 2
has_children: false
---

# Configuration Reference
{: .no_toc }

{% include toc.html %}

Configuring lakeFS is done using a yaml configuration file and/or environment variable.
The configuration file location can be set with the '--config' flag. If not specified, the the first file found in the following order will be used:
1. ./config.yaml
1. `$HOME`/lakefs/config.yaml
1. /etc/lakefs/config.yaml
1. `$HOME`/.lakefs.yaml

Configuration items can each be controlled by an environment variable. The variable name will have a prefix of *LAKEFS_*, followed by the name of the configuration, replacing every '.' with a '_'.
Example: `LAKEFS_LOGGING_LEVEL` controls `logging.level`.

This reference uses `.` to denote the nesting of values.

## Reference

* `logging.format` `(one of ["json", "text"] : "text")` - Format to output log message in
* `logging.level` `(one of ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "NONE"] : "DEBUG")` - Logging level to output
* `logging.output` `(string : "-")` - A path or paths to write logs to. A `-` means the standard output, `=` means the standard error.
* `logging.file_max_size_mb` `(int : 100)` - Output file maximum size in megabytes.
* `logging.files_keep` `(int : 0)` - Numbe of log files to keep, default is all.
* `actions.enabled` `(bool : true)` - Setting this to false will block hooks from being executed
* `database.connection_string` `(string : "postgres://localhost:5432/postgres?sslmode=disable")` - PostgreSQL connection string to use
* `database.max_open_connections` `(int : 25)` - Maximum number of open connections to the database
* `database.max_idle_connections` `(int : 25)` - Sets the maximum number of connections in the idle connection pool
* `database.connection_max_lifetime` `(duration : 5m)` - Sets the maximum amount of time a connection may be reused
* `listen_address` `(string : "0.0.0.0:8000")` - A `<host>:<port>` structured string representing the address to listen on
* `auth.cache.enabled` `(bool : true)` - Whether to cache access credentials and user policies in-memory. Can greatly improve throughput when enabled.
* `auth.cache.size` `(int : 1024)` - How many items to store in the auth cache. Systems with a very high user count should use a larger value at the expense of ~1kb of memory per cached user.
* `auth.cache.ttl` `(time duration : "20s")` - How long to store an item in the auth cache. Using a higher value reduces load on the database, but will cause changes longer to take effect for cached users.
* `auth.cache.jitter` `(time duration : "3s")` - A random amount of time between 0 and this value is added to each item's TTL. This is done to avoid a large bulk of keys expiring at once and overwhelming the database.
* `auth.encrypt.secret_key` `(string : required)` - A random (cryptographically safe) generated string that is used for encryption and HMAC signing

   **Note:** It is best to keep this somewhere safe such as KMS or Hashicorp Vault, and provide it to the system at run time
   {: .note }
* <a name="ldap"/>`auth.ldap.server_endpoint` `(string : required)` - If specified, also authenticate users via this LDAP server
* `auth.ldap.bind_dn` `(string : required)` - Use this DN to bind lakeFS on the LDAP server for searching for users.
* `auth.ldap.bind_password` `(string : )` - If set, use this password for binding `bind_dn`.
* `auth.ldap.username_attribute` `(string : required)` - Attribute holding login username on LDAP users, e.g. `cn` or `uid`.
* `auth.ldap.user_base_dn` `(string : required)` - Base DN for searching for users.  Search looks for users in the subtree below this.
* `auth.ldap.default_user_group` `(string : )` - Create all LDAP users in this group.  Defaults to `Viewers`.
* `auth.ldap.user_filter` `(string : )` - Additional filter for users.
* `blockstore.type` `(one of ["local", "s3", "gs", "azure", "mem"] : required)`.  Block adapter to use. This controls where the underlying data will be stored
* `blockstore.default_namespace_prefix` `(string : )` - Use this to help your users choose a storage namespace for their repositories. 
   If specified, the storage namespace will be filled with this default value as a prefix, when creating a repository from the UI.
   The user may still change it to something else.
* `blockstore.local.path` `(string: "~/lakefs/data")` - When using the local Block Adapter, which directory to store files in
* `blockstore.gs.credentials_file` `(string : )` - If specified will be used as a file path of the JSON file that contains your Google service account key
* `blockstore.gs.credentials_json` `(string : )` - If specified will be used as JSON string that contains your Google service account key (when credentials_file is not set)
* `blockstore.azure.storage_account` `(string : )` - If specified, will be used as the Azure storage account
* `blockstore.azure.storage_access_key` `(string : )` - If specified, will be used as the Azure storage access key
* `blockstore.azure.auth_method` `(one of ["msi", "access-key"]: "access-key" )` - Authentication method to use (msi is used for Azure AD authentication). 
* `blockstore.s3.region` `(string : "us-east-1")` - Default region for lakeFS to use when interacting with S3.
* `blockstore.s3.profile` `(string : )` - If specified, will be used as a [named credentials profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html)
* `blockstore.s3.credentials_file` `(string : )` - If specified, will be used as a [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
* `blockstore.s3.credentials.access_key_id` `(string : )` - If specified, will be used as a static set of credential
* `blockstore.s3.credentials.secret_access_key` `(string : )` - If specified, will be used as a static set of credential
* `blockstore.s3.credentials.session_token` `(string : )` - If specified, will be used as a static session token
* `blockstore.s3.endpoint` `(string : )` - If specified, custom endpoint for the AWS S3 API (https://s3_compatible_service_endpoint:port)
* `blockstore.s3.force_path_style` `(boolean : false)` - When true, use path-style S3 URLs (https://<host>/<bucket> instead of https://<bucket>.<host>)
* `blockstore.s3.streaming_chunk_size` `(int : 1048576)` - Object chunk size to buffer before streaming to blockstore (use a lower value for less reliable networks). Minimum is 8192.
* `blockstore.s3.streaming_chunk_timeout` `(time duration : "60s")` - Per object chunk timeout for blockstore streaming operations (use a larger value for less reliable networks).
* `blockstore.s3.discover_bucket_region` `(boolean : true)` - (Can be turned off if the underlying S3 bucket doesn't support the GetBucketRegion API).
* `committed.local_cache` - an object describing the local (on-disk) cache of metadata from
  permanent storage:
  + `committed.local_cache.size_bytes` (`int` : `1073741824`) - bytes for local cache to use on disk.  The cache may use more storage for short periods of time.
  + `committed.local_cache.dir` (`string`, `~/lakefs/local_tier`) - directory to store local cache.
  +	`committed.local_cache.range_proportion` (`float` : `0.9`) - proportion of local cache to
	use for storing ranges (leaves of committed metadata storage).
  + `committed.local_cache.range.open_readers` (`int` : `500`) - maximal number of unused open
    SSTable readers to keep for ranges.
  + `committed.local_cache.range.num_shards` (`int` : `30`) - sharding factor for open SSTable
    readers for ranges.  Should be at least `sqrt(committed.local_cache.range.open_readers)`.
  + `committed.local_cache.metarange_proportion` (`float` : `0.1`) - proportion of local cache
	to use for storing metaranges (roots of committed metadata storage).
  + `committed.local_cache.metarange.open_readers` (`int` : `50`) - maximal number of unused open
    SSTable readers to keep for metaranges.
  + `committed.local_cache.metarange.num_shards` (`int` : `10`) - sharding factor for open
    SSTable readers for metaranges.  Should be at least
    `sqrt(committed.local_cache.metarange.open_readers)`.
+ `committed.block_storage_prefix` (`string` : `_lakefs`) - Prefix for metadata file storage
  in each repository's storage namespace
+ `committed.permanent.min_range_size_bytes` (`int` : `0`) - Smallest allowable range in
  metadata.  Increase to somewhat reduce random access time on committed metadata, at the cost
  of increased committed metadata storage cost.
+ `committed.permanent.max_range_size_bytes` (`int` : `20971520`) - Largest allowable range in
  metadata.  Should be close to the size at which fetching from remote storage becomes linear.
+ `committed.permanent.range_raggedness_entries` (`int` : `50_000`) - Average number of object
  pointers to store in each range (subject to `min_range_size_bytes` and
  `max_range_size_bytes`).
+ `committed.sstable.memory.cache_size_bytes` (`int` : `200_000_000`) - maximal size of
  in-memory cache used for each SSTable reader.
+ `email.smtp_host` (`string` :   ) - A string representing the URL of the SMTP host.
+ `email.port` (`int` :   ) - An integer representing the port of the SMTP service (465, 587, 993, 25 are some standard ports)
+ `email.username` (`string` :   ) - A string representing the username of the specific account at the SMTP. It's recommended to provide this value at runtime from a secret vault of some sort.
+ `email.password` (`string` :   ) - A string representing the password of the account. It's recommended to provide this value at runtime from a secret vault of some sort.
+ `email.sender` (`string` :   ) - A string representing the email account which is set as the sender.
* `gateways.s3.domain_name` `(string : "s3.local.lakefs.io")` - a FQDN
  representing the S3 endpoint used by S3 clients to call this server
  (`*.s3.local.lakefs.io` always resolves to 127.0.0.1, useful for
  local development, if using [virtual-host addressing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html).
* `gateways.s3.region` `(string : "us-east-1")` - AWS region we're pretending to be. Should match the region configuration used in AWS SDK clients
* `gateways.s3.fallback_url` `(string)` - If specified, requests with a non-existing repository will be forwarded to this url. This can be useful for using lakeFS side-by-side with S3, with the URL pointing at an [S3Proxy](https://github.com/gaul/s3proxy) instance.
* `stats.enabled` `(boolean : true)` - Whether or not to periodically collect anonymous usage statistics
* `security.audit_check_interval` `(duration : 12h)` - Duration in which we check for security audit
{: .ref-list }

## Using Environment Variables

All configuration variables can be set or overridden using environment variables.
To set an environment variable, prepend `LAKEFS_` to its name, convert it to upper case, and replace `.` with `_`:

For example, `logging.format` becomes `LAKEFS_LOGGING_FORMAT`, `blockstore.s3.region` becomes `LAKEFS_BLOCKSTORE_S3_REGION`, etc.


## Example: Local Development

```yaml
---
listen_address: "0.0.0.0:8000"

database:
  connection_string: "postgres://localhost:5432/postgres?sslmode=disable"

logging:
  format: text
  level: DEBUG
  output: "-"

auth:
  encrypt:
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc09e90b6641"

blockstore:
  type: local
  local:
    path: "~/lakefs/dev/data"

gateways:
  s3:
    region: us-east-1
```


## Example: AWS Deployment

```yaml
---
logging:
  format: json
  level: WARN
  output: "-"

database:
  connection_string: "postgres://user:pass@lakefs.rds.amazonaws.com:5432/postgres"

auth:
  encrypt:
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc"

blockstore:
  type: s3
  s3:
    region: us-east-1 # optional, fallback in case discover from bucket is not supported
    credentials_file: /secrets/aws/credentials
    profile: default

```

[aws-s3-batch-permissions]: https://docs.aws.amazon.com/AmazonS3/latest/dev/batch-ops-iam-role-policies.html


## Example: Google Storage

```yaml
---
logging:
  format: json
  level: WARN
  output: "-"

database:
  connection_string: "postgres://user:pass@lakefs.rds.amazonaws.com:5432/postgres"

auth:
  encrypt:
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc"

blockstore:
  type: gs
  gs:
    credentials_file: /secrets/lakefs-service-account.json

```

## Example: MinIO

```yaml
---
logging:
  format: json
  level: WARN
  output: "-"

database:
  connection_string: "postgres://user:pass@lakefs.rds.amazonaws.com:5432/postgres"

auth:
  encrypt:
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc"

blockstore:
  type: s3
  s3:
    force_path_style: true
    endpoint: http://localhost:9000
    discover_bucket_region: false
    credentials:
      access_key_id: minioadmin
      secret_access_key: minioadmin

```
## Example: Azure blob storage

```yaml
---
logging:
  format: json
  level: WARN
  output: "-"

database:
  connection_string: "postgres://user:pass@lakefs.rds.amazonaws.com:5432/postgres"

auth:
  encrypt:
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc"

blockstore:
  type: azure
  azure:
    auth_method: access-key
    storage_account: exampleStorageAcount
    storage_access_key: ExampleAcessKeyMD7nkPOWgV7d4BUjzLw==

```

