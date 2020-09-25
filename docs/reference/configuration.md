---
layout: default
title: Configuration Reference
parent: Reference
nav_order: 2
has_children: false
---

# Configuration Reference
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

Configuring lakeFS is done using a yaml configuration file.
This reference uses `.` to denote the nesting of values.

## Reference

* `logging.format` `(one of ["json", "text"] : "text")` - Format to output log message in
* `logging.level` `(one of ["DEBUG", "INFO", "WARN", "ERROR", "NONE"] : "DEBUG")` - Logging level to output
* `logging.output` `(string : "-")` - Path name to write logs to. `"-"` means Standard Output
* `database.connection_string` `(string : "postgres://localhost:5432/postgres?sslmode=disable")` - PostgreSQL connection string to use
* `database.max_open_connections` `(int : 25)` - Maximum number of open connections to the database
* `database.max_idle_connections` `(int : 25)` - Sets the maximum number of connections in the idle connection pool
* `database.connection_max_lifetime` `(duration : 5m)` - Sets the maximum amount of time a connection may be reused
* `database.disable_auto_migrate` `(bool : false)` - Disable the database migrate to latest on connect
* `listen_address` `(string : "0.0.0.0:8000")` - A `<host>:<port>` structured string representing the address to listen on
* `auth.cache.enabled` `(bool : true)` - Whether to cache access credentials and user policies in-memory. Can greatly improve throughput when enabled.
* `auth.cache.size` `(int : 1024)` - How many items to store in the auth cache. Systems with a very high user count should use a larger value at the expense of ~1kb of memory per cached user.
* `auth.cache.ttl` `(time duration : "20s")` - How long to store an item in the auth cache. Using a higher value reduces load on the database, but will cause changes longer to take effect for cached users.
* `auth.cache.jitter` `(time duration : "3s")` - A random amount of time between 0 and this value is added to each item's TTL. This is done to avoid a large bulk of keys expiring at once and overwhelming the database.
* `auth.encrypt.secret_key` `(string : required)` - A random (cryptographically safe) generated string that is used for encryption and HMAC signing

   **Note:** It is best to keep this somewhere safe such as KMS or Hashicorp Vault, and provide it to the system at run time
   {: .note }

* `blockstore.type` `(one of ["local", "s3", "gs", "mem"]: "mem")` - Block adapter to use. This controls where the underlying data will be stored
* `blockstore.local.path` `(string: "~/lakefs/data")` - When using the local Block Adapter, which directory to store files in
* `blockstore.gs.credentials_file` `(string : )` - If specified will be used as a file path of the JSON file that contains your Google service account key
* `blockstore.gs.credentials_json` `(string : )` - If specified will be used as JSON string that contains your Google service account key (when credentials_file is not set)
* `blockstore.s3.region` `(string : "us-east-1")` - When using the S3 block adapter, AWS region to use
* `blockstore.s3.profile` `(string : )` - If specified, will be used as a [named credentials profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html)
* `blockstore.s3.credentials_file` `(string : )` - If specified, will be used as a [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
* `blockstore.s3.credentials.access_key_id` `(string : )` - If specified, will be used as a static set of credential
* `blockstore.s3.credentials.secret_access_key` `(string : )` - If specified, will be used as a static set of credential
* `blockstore.s3.credentials.session_token` `(string : )` - If specified, will be used as a static session token
* `blockstore.s3.streaming_chunk_size` `(int : 1048576)` - Object chunk size to buffer before streaming to S3 (use a lower value for less reliable networks). Minimum is 8192.
* `blockstore.s3.retention.role_arn` - ARN of IAM role to use to
  perform AWS S3 Batch tagging operations.  This role must be
  configured according to [Granting permissions for Amazon S3 Batch
  Operations][aws-s3-batch-permissions] for "PUT object tagging", with
  these permissions:
  * `ListBucket` on all buckets used for storing repositories.
  * `PutObjectTagging` and `PutObjectVersionTagging` on all buckets
     and prefixes used for storing repositories.
  * `GetObject` under `blockstore.s3.retention.manifest_base_url`,
  * `PutObject` under `blockstore.s3.retention.report_s3_prefix_url`.
* `blockstore.s3.retention.manifest_base_url` - Base S3 URL to use for
  uploading batch tagging manifest files.  Must be readable by
  `blockstore.s3.retention.role_arn` and writable by the configured
  AWS role running `lakefs`.
* `blockstore.s3.retention.report_s3_prefix_url` - Base S3 URL to use
  for writing batch tagging completion reports.  Must be writable by
  `blockstore.s3.retention.role_arn`.
* `gateways.s3.domain_name` `(string : "s3.local.lakefs.io")` - a FQDN
  representing the S3 endpoint used by S3 clients to call this server
  (`*.s3.local.lakefs.io` always resolves to 127.0.0.1, useful for
  local development
* `gateways.s3.region` `(string : "us-east-1")` - AWS region we're pretending to be. Should match the region configuration used in AWS SDK clients
* `stats.enabled` `(boolean : true)` - Whether or not to periodically collect anonymous usage statistics
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
    domain_name: s3.local.lakefs.io
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
    region: us-east-1
    credentials_file: /secrets/aws/credentials
    profile: default

gateways:
  s3:
    domain_name: s3.my-company.com
    region: us-east-1
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

gateways:
  s3:
    domain_name: s3.my-company.com
    region: us-east-1
```
