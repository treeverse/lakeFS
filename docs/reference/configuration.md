---
title: lakeFS Server Configuration
description: Configuration reference for lakeFS Server
parent: Reference
---

# lakeFS Server Configuration

{% include toc.html %}

Configuring lakeFS is done using a YAML configuration file and/or environment variable.
The configuration file's location can be set with the '--config' flag. If not specified, the first file found in the following order will be used:
1. ./config.yaml
1. `$HOME`/lakefs/config.yaml
1. /etc/lakefs/config.yaml
1. `$HOME`/.lakefs.yaml

Configuration items can each be controlled by an environment variable. The variable name will have a prefix of *LAKEFS_*, followed by the name of the configuration, replacing every '.' with a '_'.
Example: `LAKEFS_LOGGING_LEVEL` controls `logging.level`.

This reference uses `.` to denote the nesting of values.

## Reference

* `logging.format` `(one of ["json", "text"] : "text")` - Format to output log message in
* `logging.level` `(one of ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "NONE"] : "INFO")` - Logging level to output
* `logging.audit_log_level` `(one of ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "NONE"] : "DEBUG")` - Audit logs level to output.

  **Note:** In case you configure this field to be lower than the main logger level, you won't be able to get the audit logs
  {: .note }
* `logging.output` `(string : "-")` - A path or paths to write logs to. A `-` means the standard output, `=` means the standard error.
* `logging.file_max_size_mb` `(int : 100)` - Output file maximum size in megabytes.
* `logging.files_keep` `(int : 0)` - Number of log files to keep, default is all.
* `actions.enabled` `(bool : true)` - Setting this to false will block hooks from being executed.
* `actions.lua.net_http_enabled` `(bool : false)` - Setting this to true will load the `net/http` package.
* `actions.env.enabled` `(bool : true)` - Environment variables accessible by hooks, disabled values evaluated to empty strings
* `actions.env.prefix` `(string : "LAKEFSACTION_")` - Access to environment variables is restricted to those with the prefix. When environment access is enabled and no prefix is provided, all variables are accessible.

  **Note:** Deprecated - See `database` section
  {: .note }
* `database` - Configuration section for the lakeFS key-value store database
  + `database.type` `(string ["postgres"|"dynamodb"|"cosmosdb"|"local"] : )` - 
    lakeFS database type
  + `database.postgres` - Configuration section when using `database.type="postgres"`
    + `database.postgres.connection_string` `(string : "postgres://localhost:5432/postgres?sslmode=disable")` - PostgreSQL connection string to use
    + `database.postgres.max_open_connections` `(int : 25)` - Maximum number of open connections to the database
    + `database.postgres.max_idle_connections` `(int : 25)` - Maximum number of connections in the idle connection pool
    + `database.postgres.connection_max_lifetime` `(duration : 5m)` - Sets the maximum amount of time a connection may be reused `(valid units: ns|us|ms|s|m|h)`
  + `database.dynamodb` - Configuration section when using `database.type="dynamodb"`
    + `database.dynamodb.table_name` `(string : "kvstore")` - Table used to store the data
    + `database.dynamodb.scan_limit` `(int : 1025)` - Maximal number of items per page during scan operation

      **Note:** Refer to the following [AWS documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Limit) for further information
      {: .note }
    + `database.dynamodb.endpoint` `(string : )` - Endpoint URL for database instance
    + `database.dynamodb.aws_region` `(string : )` - AWS Region of database instance
    + `database.dynamodb.aws_profile` `(string : )` - AWS named profile to use
    + `database.dynamodb.aws_access_key_id` `(string : )` - AWS access key ID
    + `database.dynamodb.aws_secret_access_key` `(string : )` - AWS secret access key
    + **Note:** `endpoint` `aws_region` `aws_access_key_id` `aws_secret_access_key` are not required and used mainly for experimental purposes when working with DynamoDB with different AWS credentials.
      {: .note }
    + `database.dynamodb.health_check_interval` `(duration : 0s)` - Interval to run health check for the DynamoDB instance (won't run if equal to 0).
  + `database.cosmosdb` - Configuration section when using `database.type="cosmosdb"`
    + `database.cosmosdb.key` `(string : "")` - If specified, will 
      be used to authenticate to the CosmosDB account. Otherwise, Azure SDK 
      default authentication (with env vars) will be used.
    + `database.cosmosdb.endpoint` `(string : "")` - CosmosDB account endpoint, e.g. `https://<account>.documents.azure.com/`.
    + `database.cosmosdb.database` `(string : "")` - CosmosDB database name.
    + `database.cosmosdb.container` `(string : "")` - CosmosDB container name.
    + `database.cosmosdb.throughput` `(int32 : )` - CosmosDB container's RU/s. If not set - the default CosmosDB container throughput is used. 
    + `database.cosmosdb.autoscale` `(bool : false)` - If set, CosmosDB container throughput is autoscaled (See CosmosDB docs for minimum throughput requirement). Otherwise, uses "Manual" mode ([Docs](https://learn.microsoft.com/en-us/azure/cosmos-db/provision-throughput-autoscale)).
  + `database.local` - Configuration section when using `database.type="local"`
    + `database.local.path` `(string : "~/lakefs/metadata")` - Local path on the filesystem to store embedded KV metadata, like branches and uncommitted entries
    + `database.local.sync_writes` `(bool: true)` - Ensure each write is written to the disk. Disable to increase performance
    + `database.local.prefetch_size` `(int: 256)` - How many items to prefetch when iterating over embedded KV records
    + `database.local.enable_logging` `(bool: false)` - Enable trace logging for local driver
* `listen_address` `(string : "0.0.0.0:8000")` - A `<host>:<port>` structured string representing the address to listen on
* `tls.enabled` `(bool :false)` - Enable TLS listening. The `listen_address` will be used to serve HTTPS requests. (mainly for local development)
* `tls.cert_file` `(string : )` - Server certificate file path used while serve HTTPS (.cert or .crt file - signed certificates).
* `tls.key_file` `(string : )` - Server secret key file path used whie serve HTTPS (.key file - private key).
* `auth.cache.enabled` `(bool : true)` - Whether to cache access credentials and user policies in-memory. Can greatly improve throughput when enabled.
* `auth.cache.size` `(int : 1024)` - How many items to store in the auth cache. Systems with a very high user count should use a larger value at the expense of ~1kb of memory per cached user.
* `auth.cache.ttl` `(time duration : "20s")` - How long to store an item in the auth cache. Using a higher value reduces load on the database, but will cause changes longer to take effect for cached users.
* `auth.cache.jitter` `(time duration : "3s")` - A random amount of time between 0 and this value is added to each item's TTL. This is done to avoid a large bulk of keys expiring at once and overwhelming the database.
* `auth.encrypt.secret_key` `(string : required)` - A random (cryptographically safe) generated string that is used for encryption and HMAC signing
* `auth.login_duration` `(time duration : "168h")` - The duration the login token is valid for
* `auth.cookie_domain` `(string : "")` - [Domain attribute](https://developer.mozilla.org/en-US/docs/Web/HTTP/Cookies#define_where_cookies_are_sent) to set the access_token cookie on (the default is an empty string which defaults to the same host that sets the cookie)
* `auth.api.endpoint` `(string: https://external.service/api/v1)` - URL to external Authorization Service described at [authorization.yml](https://github.com/treeverse/lakeFS/blob/master/api/authorization.yml);
* `auth.api.token` `(string: eyJhbGciOiJIUzI1NiIsInR5...)` - API token used to authenticate requests to api endpoint
* `auth.api.health_check_timeout` `(time duration : "20s")` - Timeout duration for external auth API health check
* `auth.api.skip_health_check` `(bool : false)` - Skip external auth API health check

   **Note:** It is best to keep this somewhere safe such as KMS or Hashicorp Vault, and provide it to the system at run time
   {: .note }
* `auth.remote_authenticator.enabled` `(bool : false)` - If specified, also authenticate users via this Remote Authenticator server.
* `auth.remote_authenticator.endpoint` `(string : required)` - Endpoint URL of the remote authentication service (e.g. https://my-auth.example.com/auth).
* `auth.remote_authenticator.default_user_group` `(string : Viewers)` - Create users in this group (i.e `Viewers`, `Developers`, etc).
* `auth.remote_authenticator.request_timeout` `(duration : 10s)` - If specified, timeout for remote authentication requests.
* `auth.cookie_auth_verification.validate_id_token_claims` `(map[string]string : )` - When a user tries to access lakeFS, validate that the ID token contains these claims with the corresponding values.
* `auth.cookie_auth_verification.default_initial_groups` (string[] : [])` - By default, users will be assigned to these groups
* `auth.cookie_auth_verification.initial_groups_claim_name` `(string[] : [])` - Use this claim from the ID token to provide the initial group for new users. This will take priority if `auth.cookie_auth_verification.default_initial_groups` is also set.
* `auth.cookie_auth_verification.friendly_name_claim_name` `(string[] : )` - If specified, the value from the claim with this name will be used as the user's display name.
* `auth.cookie_auth_verification.external_user_id_claim_name` - `(string : )` - If specified, the value from the claim with this name will be used as the user's id name.
* `auth.cookie_auth_verification.auth_source` - `(string : )` - If specified, user will be labeled with this auth source.
* `auth.oidc.default_initial_groups` `(string[] : [])` - By default, OIDC users will be assigned to these groups
* `auth.oidc.initial_groups_claim_name` `(string[] : [])` - Use this claim from the ID token to provide the initial group for new users. This will take priority if `auth.oidc.default_initial_groups` is also set.
* `auth.oidc.friendly_name_claim_name` `(string[] : )` - If specified, the value from the claim with this name will be used as the user's display name.
* `auth.oidc.validate_id_token_claims` `(map[string]string : )` - When a user tries to access lakeFS, validate that the ID token contains these claims with the corresponding values.
* `auth.ui_config.rbac` `(string: "simplified")` - "simplified", "external" or "internal" (enterprise feature).  In simplified mode, do not display policy in GUI.
  If you have configured an external auth server you can set this to "external" to support the policy editor.
  If you are using the enteprrise version of lakeFS, you can set this to "internal" to use the built-in policy editor.
* `blockstore.type` `(one of ["local", "s3", "gs", "azure", "mem"] : required)`. Block adapter to use. This controls where the underlying data will be stored
* `blockstore.default_namespace_prefix` `(string : )` - Use this to help your users choose a storage namespace for their repositories.
   If specified, the storage namespace will be filled with this default value as a prefix when creating a repository from the UI.
   The user may still change it to something else.
* `blockstore.local.path` `(string: "~/lakefs/data")` - When using the local Block Adapter, which directory to store files in
* `blockstore.local.import_enabled` `(bool: false)` - Enable import for local Block Adapter, relevant only if you are using shared location
* `blockstore.local.import_hidden` `(bool: false)` - When enabled import will scan and import any file or folder that starts with a dot character.
* `blockstore.local.allowed_external_prefixes` `([]string: [])` - List of absolute path prefixes used to match any access for external location (ex: /var/data/). Empty list mean no access to external location.
* `blockstore.gs.credentials_file` `(string : )` - If specified will be used as a file path of the JSON file that contains your Google service account key
* `blockstore.gs.credentials_json` `(string : )` - If specified will be used as JSON string that contains your Google service account key (when credentials_file is not set)
* `blockstore.gs.pre_signed_expiry` `(time duration : "15m")` - Expiry of pre-signed URL.
* `blockstore.gs.disable_pre_signed` `(bool : false)` - Disable use of pre-signed URL.
* `blockstore.gs.disable_pre_signed_ui` `(bool : true)` - Disable use of pre-signed URL in the UI.
* `blockstore.azure.storage_account` `(string : )` - If specified, will be used as the Azure storage account
* `blockstore.azure.storage_access_key` `(string : )` - If specified, will be used as the Azure storage access key
* `blockstore.azure.pre_signed_expiry` `(time duration : "15m")` - Expiry of pre-signed URL.
* `blockstore.azure.disable_pre_signed` `(bool : false)` - Disable use of pre-signed URL.
* `blockstore.azure.disable_pre_signed_ui` `(bool : true)` - Disable use of pre-signed URL in the UI.
* `blockstore.azure.china_cloud` `(bool : false)` - Enable for using lakeFS on Azure China Cloud.
* `blockstore.s3.region` `(string : "us-east-1")` - Default region for lakeFS to use when interacting with S3.
* `blockstore.s3.profile` `(string : )` - If specified, will be used as a [named credentials profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-using-profiles)
* `blockstore.s3.credentials_file` `(string : )` - If specified, will be used as a [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
* `blockstore.s3.credentials.access_key_id` `(string : )` - If specified, will be used as a static set of credential
* `blockstore.s3.credentials.secret_access_key` `(string : )` - If specified, will be used as a static set of credential
* `blockstore.s3.credentials.session_token` `(string : )` - If specified, will be used as a static session token
* `blockstore.s3.endpoint` `(string : )` - If specified, custom endpoint for the AWS S3 API (https://s3_compatible_service_endpoint:port)
* `blockstore.s3.force_path_style` `(bool : false)` - When true, use path-style S3 URLs (https://<host>/<bucket> instead of https://<bucket>.<host>)
* `blockstore.s3.discover_bucket_region` `(bool : true)` - (Can be turned off if the underlying S3 bucket doesn't support the GetBucketRegion API).
* `blockstore.s3.skip_verify_certificate_test_only` `(bool : false)` - Skip certificate verification while connecting to the storage endpoint. Should be used only for testing.
* `blockstore.s3.server_side_encryption` `(string : )` - Server side encryption format used (Example on AWS using SSE-KMS while passing "aws:kms")
* `blockstore.s3.server_side_encryption_kms_key_id` `(string : )` - Server side encryption KMS key ID
* `blockstore.s3.pre_signed_expiry` `(time duration : "15m")` - Expiry of pre-signed URL.
* `blockstore.s3.disable_pre_signed` `(bool : false)` - Disable use of pre-signed URL.
* `blockstore.s3.disable_pre_signed_ui` `(bool : true)` - Disable use of pre-signed URL in the UI.
* `blockstore.s3.disable_pre_signed_multipart` `(bool : )` - Disable use of pre-signed multipart upload **experimental**, enabled on s3 block adapter with presign support.
* `blockstore.s3.client_log_request` `(bool : false)` - Set SDK logging bit to log requests
* `blockstore.s3.client_log_retries` `(bool : false)` - Set SDK logging bit to log retries
* `graveler.reposiory_cache.size` `(int : 1000)` - How many items to store in the repository cache.
* `graveler.reposiory_cache.ttl` `(time duration : "5s")` - How long to store an item in the repository cache.
* `graveler.reposiory_cache.jitter` `(time duration : "2s")` - A random amount of time between 0 and this value is added to each item's TTL.
* `graveler.ensure_readable_root_namespace` `(bool: true)` - When creating a new repository use this to verify that lakeFS has access to the root of the underlying storage namespace. Set `false` only if lakeFS should not have access (i.e pre-sign mode only).
* `graveler.commit_cache.size` `(int : 50000)` - How many items to store in the commit cache.
* `graveler.commit_cache.ttl` `(time duration : "10m")` - How long to store an item in the commit cache.
* `graveler.commit_cache.jitter` `(time duration : "2s")` - A random amount of time between 0 and this value is added to each item's TTL.
* `graveler.background.rate_limit` `(int : 0)` - Advence configuration to control background work done rate limit in requests per second (default: 0 - unlimited).
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
+ `email.smtp_host` `(string)` - A string representing the URL of the SMTP host.
+ `email.smtp_port` (`int`) - An integer representing the port of the SMTP service (465, 587, 993, 25 are some standard ports)
+ `email.use_ssl` (`bool : false`) - Use SSL connection with SMTP host.
+ `email.username` `(string)` - A string representing the username of the specific account at the SMTP. It's recommended to provide this value at runtime from a secret vault of some sort.
+ `email.password` `(string)` - A string representing the password of the account. It's recommended to provide this value at runtime from a secret vault of some sort.
+ `email.local_name` `(string)` - A string representing the hostname sent to the SMTP server with the HELO command. By default, "localhost" is sent.
+ `email.sender` `(string)` - A string representing the email account which is set as the sender.
+ `email.limit_every_duration` `(duration : 1m)` - The average time between sending emails. If zero is entered, there is no limit to the amount of emails that can be sent.
+ `email.burst` `(int: 10)` - Maximal burst of emails before applying `limit_every_duration`. The zero value means no burst and therefore no emails can be sent.
+ `email.lakefs_base_url` `(string : "http://localhost:8000")` - A string representing the base lakeFS endpoint to be directed to when emails are sent inviting users, reseting passwords etc.
* `gateways.s3.domain_name` `(string : "s3.local.lakefs.io")` - a FQDN
  representing the S3 endpoint used by S3 clients to call this server
  (`*.s3.local.lakefs.io` always resolves to 127.0.0.1, useful for
  local development, if using [virtual-host addressing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html).
* `gateways.s3.region` `(string : "us-east-1")` - AWS region we're pretending to be in, it should match the region configuration used in AWS SDK clients
* `gateways.s3.fallback_url` `(string)` - If specified, requests with a non-existing repository will be forwarded to this URL. This can be useful for using lakeFS side-by-side with S3, with the URL pointing at an [S3Proxy](https://github.com/gaul/s3proxy) instance.
* `gateways.s3.verify_unsupported` `(bool : true)` - The S3 gateway errors on unsupported requests, but when disabled, defers to target-based handlers.
* `stats.enabled` `(bool : true)` - Whether to periodically collect anonymous usage statistics
* `stats.flush_interval` `(duration : 30s)` - Interval used to post anonymous statistics collected
* `stats.flush_size` `(int : 100)` - A size (in records) of anonymous statistics collected in which we post
* `security.audit_check_interval` `(duration : 24h)` - Duration in which we check for security audit.
* `ui.enabled` `(bool: true)` - Whether to server the embedded UI from the binary
* `ugc.prepare_max_file_size` `(int: 125829120)` - Uncommitted garbage collection prepare request, limit the produced file maximum size
* `ugc.prepare_interval` `(duraction: 1m)` - Uncommitted garbage collection prepare request, limit produce time to interval
* `installation.user_name` `(string : )` - When specified, an initial admin user will be created when the server is first run. Works only when `database.type` is set to local. Requires `installation.access_key_id` and `installation.secret_access_key`. 
* `installation.access_key_id` `(string : )` - Admin's initial access key id (used once in the initial setup process)
* `installation.secret_access_key` `(string : )` - Admin's initial secret access key (used once in the initial setup process)
* `usage_report.enabled` `(bool : false)` - Store API and Gateway usage reports into key-value store.
* `usage_report.flush_interval` `(duration : 5m)` - Sets interval for flushing in-memory usage data to key-value store.

{: .ref-list }

## Using Environment Variables

All the configuration variables can be set or overridden using environment variables.
To set an environment variable, prepend `LAKEFS_` to its name, convert it to upper case, and replace `.` with `_`:

For example, `logging.format` becomes `LAKEFS_LOGGING_FORMAT`, `blockstore.s3.region` becomes `LAKEFS_BLOCKSTORE_S3_REGION`, etc.


## Example Configurations

### Local Development with PostgreSQL database

```yaml
---
listen_address: "0.0.0.0:8000"

database:
  type: "postgres"
  postgres:
    connection_string: "postgres://localhost:5432/postgres?sslmode=disable"

logging:
  format: text
  level: DEBUG
  output: "-"

auth:
  encrypt:
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc"

blockstore:
  type: local
  local:
    path: "~/lakefs/dev/data"

gateways:
  s3:
    region: us-east-1
```


### AWS Deployment with DynamoDB database

```yaml
---
logging:
  format: json
  level: WARN
  output: "-"

database:
  type: "dynamodb"
  dynamodb:
    table_name: "kvstore"

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

### Google Storage

```yaml
---
logging:
  format: json
  level: WARN
  output: "-"

database:
  type: "postgres"
  postgres:
    connection_string: "postgres://user:pass@lakefs.rds.amazonaws.com:5432/postgres"

auth:
  encrypt:
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc"

blockstore:
  type: gs
  gs:
    credentials_file: /secrets/lakefs-service-account.json

```

### MinIO

```yaml
---
logging:
  format: json
  level: WARN
  output: "-"

database:
  type: "postgres"
  postgres:
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
### Azure blob storage

```yaml
---
logging:
  format: json
  level: WARN
  output: "-"

database:
  type: "cosmosdb"
  cosmosdb:
    key: "ExampleReadWriteKeyMD7nkPOWgV7d4BUjzLw=="
    endpoint: "https://lakefs-account.documents.azure.com:443/"
    database: "lakefs-db"
    container: "lakefs-container"

auth:
  encrypt:
    secret_key: "10a718b3f285d89c36e9864494cdd1507f3bc85b342df24736ea81f9a1134bcc"

blockstore:
  type: azure
  azure:
    storage_account: exampleStorageAcount
    storage_access_key: ExampleAcessKeyMD7nkPOWgV7d4BUjzLw==

```
