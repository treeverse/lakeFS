---
title: Configuration Reference
description: a configuration reference for lakeFS Enterprise
---

# lakeFS Enterprise Configuration Reference


lakeFS Enterprise configuration extends lakeFS's configuration and uses the same config file. 

## lakeFS Configuration

See the full [lakeFS Server Configuration](../reference/configuration.md)


### Reference

This reference uses `.` to denote the nesting of values.

* `license` - Configuration section for licensing
  + `license.path` `(string)` - The file system path to the license token file, e.g. '/path/to/your/license/file/license.txt'
  + `license.contents` `(string)` - The license token string provided directly in configuration, e.g. 'eyJhbGciOiJSUzI1NiIs...'

!!! note
    If both `license.path` and `license.contents` are provided or if neither is set, lakeFS Enterprise will fail to start with an error. You should provide only one. The `license.path` is the preferred option.

* `auth` - Configuration section for authentication services, like SAML or OIDC.
  + `auth.logout_redirect_url` `(string : "/auth/login")` - The address to redirect to after a successful logout, e.g. login.
  + `auth.ui_config` Configuration section for UI authentication settings
    + `auth.ui_config.rbac` `(string: "none")` - The RBAC mode to use for authorization, options: "none", "simplified", "external" or "internal" 
    + `auth.ui_config.login_url` `(string : "")` - The URL to redirect users on login
    + `auth.ui_config.login_failed_message` `(string : "")` - Custom message to display when login fails
    + `auth.ui_config.fallback_login_url` `(string)` - Alternative login URL to use as fallback
    + `auth.ui_config.fallback_login_label` `(string)` - Label text for the fallback login option
    + `auth.ui_config.login_cookie_names` `([]string : ["internal_auth_session"])` - The name of the cookie(s) lakeFS will set following a successful authentication. The value is the authenticated user's JWT
    + `auth.ui_config.logout_url` `(string : "")` - The URL to redirect users on logout
    + `auth.ui_config.use_login_placeholders` `(bool : false)` - Whether to use placeholder text in login forms
  + `auth.ldap`
    + `auth.ldap.server_endpoint` `(string : required)` - The LDAP server address, e.g. 'ldaps://ldap.company.com:636'
    + `auth.ldap.bind_dn` `(string : required)` - The bind string, e.g. 'uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com'
    + `auth.ldap.bind_password` `(string : required)` - The password for the user to bind.
    + `auth.ldap.username_attribute` `(string : required)` - The user name attribute, e.g. 'uid'
    + `auth.ldap.user_base_dn` `(string : required)` - The search request base dn, e.g. 'ou=Users,o=<org-id>,dc=<company>,dc=com'
    + `auth.ldap.user_filter` `(string : required)` - The search request user filter, e.g. '(objectClass=inetOrgPerson)'
    + `auth.ldap.connection_timeout_seconds` `(int : required)` - The timeout for a single connection
    + `auth.ldap.request_timeout_seconds` `(int : required)` - The timeout for a single request
    + `auth.ldap.default_user_group` `(string : "")` - The default group for the users initially authenticated by the remote service
  + `auth.saml` Configuration section for SAML
    + `auth.saml.sp_root_url` `(string : '')` - The base lakeFS-URL, e.g. 'https://<lakefs-url>'
    + `auth.saml.sp_x509_key_path` `(string : '')` - The path to the private key, e.g '/etc/saml_certs/rsa_saml_private.cert'
    + `auth.saml.sp_x509_cert_path` `(string : '')` - The path to the public key, '/etc/saml_certs/rsa_saml_public.pem'
    + `auth.saml.sp_sign_request` `(bool : 'false')` SPSignRequest some IdP require the SLO request to be signed
    + `auth.saml.sp_signature_method` `(string : '')` SPSignatureMethod optional valid signature values depending on the IdP configuration, e.g. 'http://www.w3.org/2001/04/xmldsig-more#rsa-sha256'
    + `auth.saml.idp_metadata_url` `(string : '')` - The URL for the metadata server, e.g. 'https://<adfs-auth.company.com>/federationmetadata/2007-06/federationmetadata.xml'
    + `auth.saml.idp_metadata_file_path` `(string : '')` - The path to the Identity Provider (IdP) metadata XML file, e.g. '/etc/saml/idp-metadata.xml'
    + `auth.saml.idp_skip_verify_tls_cert` `(bool : false)` - Insecure skip verification of the IdP TLS certificate, like when signed by a private CA
    + `auth.saml.idp_authn_name_id_format` `(string : 'urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified')` - The format used in the NameIDPolicy for authentication requests
    + `auth.saml.idp_request_timeout` `(duration : '10s')` The timeout for remote authentication requests.
    + `auth.saml.post_login_redirect_url` `(string : '')` - The URL to redirect users to after successful SAML authentication, e.g. 'http://localhost:8000/'
  + `auth.oidc` Configuration section for OIDC
    + `auth.oidc.enabled` `(bool : false)` - Enables OIDC Authentication.
    + `auth.oidc.url` `(string : '')` - The OIDC provider url, e.g. 'https://oidc-provider-url.com/'
    + `auth.oidc.client_id` `(string : '')` - The application's ID.
    + `auth.oidc.client_secret` `(string : '')` - The application's secret.
    + `auth.oidc.callback_base_url` `(string : '')` - A default callback address of the lakeFS server.
    + `auth.oidc.callback_base_urls` `(string[] : '[]')` - If callback_base_urls is configured, check current host is whitelisted otherwise use callback_base_url (without 's'). These config keys are mutually exclusive

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

### Using Environment Variables

All the configuration variables can be set or overridden using environment variables.
To set an environment variable, prepend `LAKEFS_` to its name, convert it to upper case, and replace `.` with `_`:

For example, `auth.logout_redirect_url` becomes `LAKEFS_AUTH_LOGOUT_REDIRECT_URL`, `auth.external.aws_auth.enabled` becomes `LAKEFS_AUTH_EXTERNAL_AWS_AUTH_ENABLED`, etc.

To set a value for a `map[string]string` type field, use the syntax `key1=value1,key2=value2,...`.
