---
title: Configuration Reference
description: A configuration reference for lakeFS Enterprise
---

# lakeFS Enterprise Configuration Reference


lakeFS Enterprise configuration extends lakeFS's configuration and uses the same config file. 

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


### Reference

This reference uses `.` to denote the nesting of values.


### auth

Configuration section for authentication services, like SAML or OIDC.

* `auth.logout_redirect_url` `(string : "/auth/login")` - The URL to redirect to after logout. The behavior depends on the authentication provider:
  - **For OIDC**: The logout URL of the OIDC provider (e.g., Auth0 logout endpoint)
  - **For SAML**: The URL within lakeFS where the IdP should redirect after logout (e.g., `/auth/login`)

### auth.providers

Configuration section external identity providers

#### auth.providers.ldap

* `auth.providers.ldap.server_endpoint` `(string : "")` - The LDAP server address, e.g. `'ldaps://ldap.company.com:636'`
* `auth.providers.ldap.bind_dn` `(string : "")` - The bind string, e.g. `'uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com'`
* `auth.providers.ldap.bind_password` `(string : "")` - The password for the user to bind
* `auth.providers.ldap.username_attribute` `(string : "")` - The user name attribute, e.g. 'uid'
* `auth.providers.ldap.user_base_dn` `(string : "")` - The search request base dn, e.g. `'ou=Users,o=<org-id>,dc=<company>,dc=com'`
* `auth.providers.ldap.user_filter` `(string : "")` - The search request user filter, e.g. `'(objectClass=inetOrgPerson)'`
* `auth.providers.ldap.connection_timeout_seconds` `(int : 0)` - The timeout for a single connection
* `auth.providers.ldap.request_timeout_seconds` `(int : 0)` - The timeout for a single request
* `auth.providers.ldap.default_user_group` `(string : "")` - The default group for the users initially authenticated by the remote service

#### auth.providers.saml

Configuration section for SAML

* `auth.providers.saml.sp_root_url` `(string : '')` - The base lakeFS-URL, e.g. `'https://<lakefs-url>'`
* `auth.providers.saml.sp_x509_key_path` `(string : '')` - The path to the private key, e.g `'/etc/saml_certs/rsa_saml_private.cert'`
* `auth.providers.saml.sp_x509_cert_path` `(string : '')` - The path to the public key, '/etc/saml_certs/rsa_saml_public.pem'
* `auth.providers.saml.sp_sign_request` `(bool : false)` Some IdP require the SLO request to be signed
* `auth.providers.saml.sp_signature_method` `(string : '')` Optional valid signature values depending on the IdP configuration, e.g. 'http://www.w3.org/2001/04/xmldsig-more#rsa-sha256'
* `auth.providers.saml.idp_metadata_url` `(string : '')` - The URL for the metadata server, e.g. `'https://<adfs-auth.company.com>/federationmetadata/2007-06/federationmetadata.xml'`
* `auth.providers.saml.idp_metadata_file_path` `(string : '')` - The path to the Identity Provider (IdP) metadata XML file, e.g. '/etc/saml/idp-metadata.xml'
* `auth.providers.saml.idp_skip_verify_tls_cert` `(bool : false)` - Insecure skip verification of the IdP TLS certificate, like when signed by a private CA
* `auth.providers.saml.idp_authn_name_id_format` `(string : 'urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified')` - The format used in the NameIDPolicy for authentication requests
* `auth.providers.saml.idp_request_timeout` `(duration : '10s')` The timeout for remote authentication requests
* `auth.providers.saml.post_login_redirect_url` `(string : '')` - The URL to redirect users to after successful SAML authentication, e.g. `'http://localhost:8000/'`

#### auth.providers.oidc

Configuration section for OIDC

* `auth.providers.oidc.url` `(string : '')` - The OIDC provider url, e.g. `'https://oidc-provider-url.com/'`
* `auth.providers.oidc.client_id` `(string : '')` - The application's ID
* `auth.providers.oidc.client_secret` `(string : '')` - The application's secret
* `auth.providers.oidc.callback_base_url` `(string : '')` - A default callback address of the lakeFS server
* `auth.providers.oidc.callback_base_urls` `(string[] : '[]')` - If callback_base_urls is configured, check current host is whitelisted otherwise use callback_base_url (without 's'). These config keys are mutually exclusive

!!! note
    You may configure a list of URLs that the OIDC provider may redirect to. This allows lakeFS to be accessed from multiple hostnames while retaining federated auth capabilities.
    If the provider redirects to a URL not in this list, the login will fail. This property and callback_base_url are mutually exclusive.

* `auth.providers.oidc.authorize_endpoint_query_parameters` `(map[string]string : {} )` - key/value parameters that are passed to a provider's authorization endpoint
* `auth.providers.oidc.logout_endpoint_query_parameters` `(string[] : [])` - The query parameters that will be used to redirect the user to the OIDC provider after logout, e.g. `["returnTo", "https://<lakefs.ingress.domain>/oidc/login"]`
* `auth.providers.oidc.logout_client_id_query_parameter` `(string : '')` - The claim name that represents the client identifier in the OIDC provider
* `auth.providers.oidc.additional_scope_claims` `(string[] : '[]')` - Specifies optional requested permissions, other than `openid` and `profile` that are being used
* `auth.providers.oidc.post_login_redirect_url` `(string : '')` - The URL to redirect users to after successful OIDC authentication, e.g. `'http://localhost:8000/'`

### auth.external

Configuration section for the external authentication methods

#### auth.external.aws_auth

Configuration section for authenticating to lakeFS using AWS presign get-caller-identity request: [External Principals AWS Auth](../security/external-principals-aws.md)

* `auth.external.aws_auth.enabled` `(bool : false)` - If true, external principals API will be enabled, e.g auth service and login api's
* `auth.external.aws_auth.get_caller_identity_max_age` `(duration : 15m)` - The maximum age in seconds for the GetCallerIdentity request to be valid, the max is 15 minutes enforced by AWS, smaller TTL can be set
* `auth.external.aws_auth.valid_sts_hosts` `([]string)` - The default are all the valid AWS STS hosts (`sts.amazonaws.com`, `sts.us-east-2.amazonaws.com` etc.)
* `auth.external.aws_auth.required_headers` `(map[string]string : )` - Headers that must be present by the client when doing login request. For security reasons it is recommended to set `X-LakeFS-Server-ID: <lakefs.ingress.domain>`, lakeFS clients assume that's the default
* `auth.external.aws_auth.optional_headers` `(map[string]string : )` - Optional headers that can be present by the client when doing login request
* `auth.external.aws_auth.http_client.timeout` `(duration : 10s)` - The timeout for the HTTP client used to communicate with AWS STS
* `auth.external.aws_auth.http_client.skip_verify` `(bool : false)` - Skip SSL verification with AWS STS

### features

* `features.local_rbac` `(bool: true)` - Backward compatibility if you use an external RBAC service (such as legacy fluffy). If `false` lakeFS will expect to use `auth.api` and all fluffy related configuration for RBAC.


* `iceberg_catalog` - Configuration section for the Iceberg REST Catalog
  + `iceberg_catalog.token_duration` `(duration : 1h)` - Authenticated token duration


### Using Environment Variables

All the configuration variables can be set or overridden using environment variables.
To set an environment variable, prepend `LAKEFS_` to its name, convert it to upper case, and replace `.` with `_`:

For example, `auth.logout_redirect_url` becomes `LAKEFS_AUTH_LOGOUT_REDIRECT_URL`, `auth.external.aws_auth.enabled` becomes `LAKEFS_AUTH_EXTERNAL_AWS_AUTH_ENABLED`, etc.

To set a value for a `map[string]string` type field, use the syntax `key1=value1,key2=value2,...`.
