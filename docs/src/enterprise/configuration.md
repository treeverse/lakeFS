---
title: Configuration Reference
description: a configuration reference for lakeFS Enterprise
---

# lakeFS Enterprise Configuration Reference

!!! warning
    fluffy will be deprecated in the upcoming versions and all functionality will be migrated into lakeFS Enterprise

Working with lakeFS Enterprise involve configuring both lakeFS and Fluffy. You can find the extended configuration references for both components below.

## lakeFS Configuration

For a complete list of configuration options, see the [lakeFS Server Configuration](../reference/configuration.md).
The sections below provide additional configuration references that complement the main configuration guide.

### blockstores

!!! info
    The `blockstores` configuration is required for [multi-storage backend](../howto/multiple-storage-backends.md) setups and replaces the previous `blockstore` configuration.

* `blockstores.signing.secret_key` `(string : required)` - A random generated string that is used for HMAC signing when using get/link physical address
* `blockstores.stores` `([{id: string, type: string, ...}] : required)` - Defines multiple storage backends used in a multi-storage backend setup. Each storage backend must have a unique id and a valid configuration.

#### Common Fields for All Stores

* `blockstores.stores[].id` `(string : required)` - Unique identifier for the storage backend.
* `blockstores.stores[].backward_compatible` `(bool : false)` - Optional. Defaults to false. Used to migrate from a single to a multi-storage backend setup.
* `blockstores.stores[].description` `(string : )` - A human-readable description of the storage backend.
* `blockstores.stores[].type` `(string : required)` - `(one of ["local", "s3", "gs", "azure", "mem"] : required)`. Block adapter to use. This controls where the underlying data will be stored.

=== "`blockstores.stores.local`"

    * `blockstores.stores[].local.path` `(string: "~/lakefs/data")` - When using the local Block Adapter, which directory to store files in
    * `blockstores.stores[].local.import_enabled` `(bool: false)` - Enable import for local Block Adapter, relevant only if you are using shared location
    * `blockstores.stores[].local.import_hidden` `(bool: false)` - When enabled import will scan and import any file or folder that starts with a dot character.
    * `blockstores.stores[].local.allowed_external_prefixes` `([]string: [])` - List of absolute path prefixes used to match any access for external location (ex: /var/data/). Empty list mean no access to external location.

=== "`blockstores.stores.s3`"

    * `blockstores.stores[].s3.region` `(string : "us-east-1")` - Default region for lakeFS to use when interacting with S3.
    * `blockstores.stores[].s3.profile` `(string : )` - If specified, will be used as a [named credentials profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-using-profiles)
    * `blockstores.stores[].credentials_file` `(string : )` - If specified, will be used as a [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
    * `blockstores.stores[].credentials.access_key_id` `(string : )` - If specified, will be used as a static set of credential
    * `blockstores.stores[].credentials.secret_access_key` `(string : )` - If specified, will be used as a static set of credential
    * `blockstores.stores[].s3.credentials.session_token` `(string : )` - If specified, will be used as a static session token
    * `blockstores.stores[].s3.endpoint` `(string : )` - If specified, custom endpoint for the AWS S3 API (https://s3_compatible_service_endpoint:port)
    * `blockstores.stores[].s3.force_path_style` `(bool : false)` - When true, use path-style S3 URLs (https://<host>/<bucket> instead of https://<bucket>.<host>)
    * `blockstores.stores[].s3.discover_bucket_region` `(bool : true)` - (Can be turned off if the underlying S3 bucket doesn't support the GetBucketRegion API).
    * `blockstores.stores[].s3.skip_verify_certificate_test_only` `(bool : false)` - Skip certificate verification while connecting to the storage endpoint. Should be used only for testing.
    * `blockstores.stores[].s3.server_side_encryption` `(string : )` - Server side encryption format used (Example on AWS using SSE-KMS while passing "aws:kms")
    * `blockstores.stores[].s3.server_side_encryption_kms_key_id` `(string : )` - Server side encryption KMS key ID
    * `blockstores.stores[].s3.pre_signed_expiry` `(time duration : "15m")` - Expiry of pre-signed URL.
    * `blockstores.stores[].s3.pre_signed_endpoint` `(string : )` - Custom endpoint for pre-signed URLs.
    * `blockstores.stores[].s3.disable_pre_signed` `(bool : false)` - Disable use of pre-signed URL.
    * `blockstores.stores[].s3.disable_pre_signed_ui` `(bool : true)` - Disable use of pre-signed URL in the UI.
    * `blockstores.stores[].s3.disable_pre_signed_multipart` `(bool : )` - Disable use of pre-signed multipart upload **experimental**, enabled on S3 block adapter with presign support.
    * `blockstores.stores[].s3.client_log_request` `(bool : false)` - Set SDK logging bit to log requests
    * `blockstores.stores[].s3.client_log_retries` `(bool : false)` - Set SDK logging bit to log retries

=== "`blockstores.azure`"

    * `blockstores.stores[].azure.storage_account` `(string : )` - If specified, will be used as the Azure storage account
    * `blockstores.stores[].azure.storage_access_key` `(string : )` - If specified, will be used as the Azure storage access key
    * `blockstores.stores[].azure.pre_signed_expiry` `(time duration : "15m")` - Expiry of pre-signed URL.
    * `blockstores.stores[].azure.disable_pre_signed` `(bool : false)` - Disable use of pre-signed URL.
    * `blockstores.stores[].azure.disable_pre_signed_ui` `(bool : true)` - Disable use of pre-signed URL in the UI.
    * `blockstores.stores[].azure.domain` `(string : blob.core.windows.net)` - Enables support of different Azure cloud domains. Current supported domains (in Beta stage): [`blob.core.chinacloudapi.cn`, `blob.core.usgovcloudapi.net`]

=== "`blockstores.gs`"

    * `blockstores.stores[].gs.credentials_file` `(string : )` - If specified will be used as a file path of the JSON file that contains your Google service account key
    * `blockstores.stores[].gs.credentials_json` `(string : )` - If specified will be used as JSON string that contains your Google service account key (when credentials_file is not set)
    * `blockstores.stores[].gs.pre_signed_expiry` `(time duration : "15m")` - Expiry of pre-signed URL.
    * `blockstores.stores[].gs.disable_pre_signed` `(bool : false)` - Disable use of pre-signed URL.
    * `blockstores.stores[].gs.disable_pre_signed_ui` `(bool : true)` - Disable use of pre-signed URL in the UI.
    * `blockstores.stores[].gs.server_side_encryption_customer_supplied` `(string : )` - Server side encryption with AES key in hex format, exclusive with key ID below
    * `blockstores.stores[].gs.server_side_encryption_kms_key_id` `(string : )` - Server side encryption KMS key ID, exclusive with above

## Fluffy Server Configuration

Configuring Fluffy using a YAML configuration file and/or environment variables.
The configuration file's location can be set with the '--config' flag. If not specified, the first file found in the following order will be used:

1. ./config.yaml
1. `$HOME`/fluffy/config.yaml
1. /etc/fluffy/config.yaml
1. `$HOME`/.fluffy.yaml

Configuration items can be controlled by environment variables, see [below](#using-environment-variables).


### Reference

This reference uses `.` to denote the nesting of values.

* `logging.format` `(one of ["json", "text"] : "text")` - Format to output log message in
* `logging.level` `(one of ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "NONE"] : "INFO")` - Logging level to output
* `logging.audit_log_level` `(one of ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "NONE"] : "DEBUG")` - Audit logs level to output.

    !!! note 
        In case you configure this field to be lower than the main logger level, you won't be able to get the audit logs
    
* `logging.output` `(string : "-")` - A path or paths to write logs to. A `-` means the standard output, `=` means the standard error.
* `logging.file_max_size_mb` `(int : 100)` - Output file maximum size in megabytes.
* `logging.files_keep` `(int : 0)` - Number of log files to keep, default is all.
* `logging.trace_request_headers` `(bool : false)` - If set to `true` and logging level is set to `TRACE`, logs request headers.
* `listen_address` `(string : "0.0.0.0:8000")` - A `<host>:<port>` structured string representing the address to listen on
* `database` - Configuration section for the Fluffy key-value store database. The database must be shared between lakeFS & Fluffy
  + `database.type` `(string ["postgres"|"dynamodb"|"cosmosdb"|"local"] : )` - Fluffy database type
  + `database.postgres` - Configuration section when using `database.type="postgres"`
    + `database.postgres.connection_string` `(string : "postgres://localhost:5432/postgres?sslmode=disable")` - PostgreSQL connection string to use
    + `database.postgres.max_open_connections` `(int : 25)` - Maximum number of open connections to the database
    + `database.postgres.max_idle_connections` `(int : 25)` - Maximum number of connections in the idle connection pool
    + `database.postgres.connection_max_lifetime` `(duration : 5m)` - Sets the maximum amount of time a connection may be reused `(valid units: ns|us|ms|s|m|h)`
  + `database.dynamodb` - Configuration section when using `database.type="dynamodb"`
    + `database.dynamodb.table_name` `(string : "kvstore")` - Table used to store the data
    + `database.dynamodb.scan_limit` `(int : 1025)` - Maximal number of items per page during scan operation

        !!! note 
            Refer to the following [AWS documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Limit) for further information
      
    + `database.dynamodb.endpoint` `(string : )` - Endpoint URL for database instance
    + `database.dynamodb.aws_region` `(string : )` - AWS Region of database instance
    + `database.dynamodb.aws_profile` `(string : )` - AWS named profile to use
    + `database.dynamodb.aws_access_key_id` `(string : )` - AWS access key ID
    + `database.dynamodb.aws_secret_access_key` `(string : )` - AWS secret access key

        !!! note
            `endpoint` `aws_region` `aws_access_key_id` `aws_secret_access_key` are not required and used mainly for experimental purposes when working with DynamoDB with different AWS credentials.
      
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
        + `database.local.path` `(string : "~/fluffy/metadata")` - Local path on the filesystem to store embedded KV metadata
        + `database.local.sync_writes` `(bool: true)` - Ensure each write is written to the disk. Disable to increase performance
        + `database.local.prefetch_size` `(int: 256)` - How many items to prefetch when iterating over embedded KV records
        + `database.local.enable_logging` `(bool: false)` - Enable trace logging for local driver
* `auth` - Configuration section for the Fluffy authentication services, like SAML or OIDC.
  + `auth.encrypt.secret_key` `(string : required)` - Same value given to lakeFS. A random (cryptographically safe) generated string that is used for encryption and HMAC signing
  + `auth.logout_redirect_url` `(string : "/auth/login")` - The address to redirect to after a successful logout, e.g. login.
  + `auth.post_login_redirect_url` `(string : '')` - Required when SAML is enabled. The address to redirect after a successful login. For most common configurations, setting to `/` will redirect to lakeFS homepage.
  + `auth.serve_listen_address` `(string : '')` - If set, an endpoint serving RBAC requests binds to this address.
  + `auth.serve_disable_authentication` `(bool : false)` - Unsafe. Disables authentication to the RBAC server.
  + `auth.ldap`
    + `auth.ldap.server_endpoint` `(string : required)` - The LDAP server address, e.g. 'ldaps://ldap.company.com:636'
    + `auth.ldap.bind_dn` `(string : required)` - The bind string, e.g. 'uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com'
    + `auth.ldap.bind_password` `(string : required)` - The password for the user to bind.
    + `auth.ldap.username_attribute` `(string : required)` - The user name attribute, e.g. 'uid'
    + `auth.ldap.user_base_dn` `(string : required)` - The search request base dn, e.g. 'ou=Users,o=<org-id>,dc=<company>,dc=com'
    + `auth.ldap.user_filter` `(string : required)` - The search request user filter, e.g. '(objectClass=inetOrgPerson)'
    + `auth.ldap.connection_timeout_seconds` `(int : required)` - The timeout for a single connection
    + `auth.ldap.request_timeout_seconds` `(int : required)` - The timeout for a single request
  + `auth.saml` Configuration section for SAML
    + `auth.saml.enabled` `(bool : false)` - Enables SAML Authentication.
    + `auth.saml.sp_root_url` `(string : '')` - The base lakeFS-URL, e.g. 'https://<lakefs-url>'
    + `auth.saml.sp_x509_key_path` `(string : '')` - The path to the private key, e.g '/etc/saml_certs/rsa_saml_private.cert'
    + `auth.saml.sp_x509_cert_path` `(string : '')` - The path to the public key, '/etc/saml_certs/rsa_saml_public.pem'
    + `auth.saml.sp_sign_request` `(bool : 'false')` SPSignRequest some IdP require the SLO request to be signed
    + `auth.saml.sp_signature_method` `(string : '')` SPSignatureMethod optional valid signature values depending on the IdP configuration, e.g. 'http://www.w3.org/2001/04/xmldsig-more#rsa-sha256'
    + `auth.saml.idp_metadata_url` `(string : '')` - The URL for the metadata server, e.g. 'https://<adfs-auth.company.com>/federationmetadata/2007-06/federationmetadata.xml'
    + `auth.saml.idp_skip_verify_tls_cert` `(bool : false)` - Insecure skip verification of the IdP TLS certificate, like when signed by a private CA
    + `auth.saml.idp_authn_name_id_format` `(string : 'urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified')` - The format used in the NameIDPolicy for authentication requests
    + `auth.saml.idp_request_timeout` `(duration : '10s')` The timeout for remote authentication requests.
    + `auth.saml.external_user_id_claim_name` `(string : '')` - The claim name to use as the user identifier with an IdP mostly for logout
  + `auth.oidc` Configuration section for OIDC
    + `auth.oidc.enabled` `(bool : false)` - Enables OIDC Authentication.
    + `auth.oidc.url` `(string : '')` - The OIDC provider url, e.g. 'https://oidc-provider-url.com/'
    + `auth.oidc.client_id` `(string : '')` - The application's ID.
    + `auth.oidc.client_secret` `(string : '')` - The application's secret.
    + `auth.oidc.callback_base_url` `(string : '')` - A default callback address of the Fluffy server.
    + `auth.oidc.callback_base_urls` `(string[] : '[]')`

        !!! note
            You may configure a list of URLs that the OIDC provider may redirect to. This allows lakeFS to be accessed from multiple hostnames while retaining federated auth capabilities.
            If the provider redirects to a URL not in this list, the login will fail. This property and callback_base_url are mutually exclusive.

    + `auth.oidc.authorize_endpoint_query_parameters` `(bool : map[string]string)` - key/value parameters that are passed to a provider's authorization endpoint.
    + `auth.oidc.logout_endpoint_query_parameters` `(string[] : '[]')` - The query parameters that will be used to redirect the user to the OIDC provider after logout, e.g. '[returnTo, https://<lakefs.ingress.domain>/oidc/login]'
    + `auth.oidc.logout_client_id_query_parameter` `(string : '')` - The claim name that represents the client identifier in the OIDC provider
    + `auth.oidc.additional_scope_claims` `(string[] : '[]')` - Specifies optional requested permissions, other than `openid` and `profile` that are being used.
  + `auth.cache` Configuration section for RBAC service cache
    + `auth.cache.enabled` `(bool : true)` - Enables RBAC service cache
    + `auth.cache.size` `(int : 1024)` - Number of users, policies and credentials to cache.
    + `auth.cache.ttl` `(duration : 20s)` - Cache items time to live expiry.
    + `auth.cache.jitter` `(duration : 3s)` - Cache items time to live jitter.
  + `auth.external` - Configuration section for the external authentication methods
    + `auth.external.aws_auth` - Configuration section for authenticating to lakeFS using AWS presign get-caller-identity request: [External Principals AWS Auth](../security/external-principals-aws.md)
      + `auth.external.aws_auth.enabled` `(bool : false)` - If true, external principals API will be enabled, e.g auth service and login api's.
      + `auth.external.aws_auth.get_caller_identity_max_age` `(duration : 15m)` - The maximum age in seconds for the GetCallerIdentity request to be valid, the max is 15 minutes enforced by AWS, smaller TTL can be set.
      + `auth.authentication_api.external_principals_enabled` `(bool : false)` - If true, external principals API will be enabled, e.g auth service and login api's.
      + `auth.external.aws_auth.valid_sts_hosts` `([]string)` - The default are all the valid AWS STS hosts (`sts.amazonaws.com`, `sts.us-east-2.amazonaws.com` etc).
      + `auth.external.aws_auth.required_headers` `(map[string]string : )` - Headers that must be present by the client when doing login request (e.g `X-LakeFS-Server-ID: <lakefs.ingress.domain>`).
      + `auth.external.aws_auth.optional_headers` `(map[string]string : )` - Optional headers that can be present by the client when doing login request.
      + `auth.external.aws_auth.http_client.timeout` `(duration : 10s)` - The timeout for the HTTP client used to communicate with AWS STS.
      + `auth.external.aws_auth.http_client.skip_verify` `(bool : false)` - Skip SSL verification with AWS STS.

* `iceberg_catalog` - Configuration section for the Iceberg REST Catalog
  + `iceberg_catalog.token_duration` `(duration : 1h)` - Authenticated token duration


### Using Environment Variables

All the configuration variables can be set or overridden using environment variables.
To set an environment variable, prepend `FLUFFY_` to its name, convert it to upper case, and replace `.` with `_`:

For example, `logging.format` becomes `FLUFFY_LOGGING_FORMAT`, `auth.saml.enabled` becomes `FLUFFY_AUTH_SAML_ENABLED`, etc.

To set a value for a `map[string]string` type field, use the syntax `key1=value1,key2=value2,...`.
