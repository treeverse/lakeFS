---
title: Install
description: lakeFS Enterprise Installation Guide
---

# Install

For production deployments of lakeFS Enterprise, follow this guide.

## lakeFS Enterprise Architecture

We recommend reviewing the [lakeFS Enterprise architecture][lakefs-enterprise-architecture] to understand the components you will be deploying.
!!! note
    Fluffy service is deprecated in chart version 1.5.0 and later.
    For more information, see the [Upgrade Guide][lakefs-enterprise-upgrade].

## Deploy lakeFS Enterprise on Kubernetes

The guide is using the [lakeFS Helm Chart](https://github.com/treeverse/charts/tree/master/charts/lakefs) to deploy a fully functional lakeFS Enterprise.

The guide includes example configurations, follow the steps below and adjust the example configurations according to:

* The platform you run on: among the platform [supported by lakeFS](../../howto/deploy/index.md#deployment-and-setup-details)
* Type of KV store you use
* Your SSO IdP and protocol

---

### Prerequisites

1. You have a Kubernetes cluster running in one of the platforms [supported by lakeFS](../../howto/deploy/index.md#deployment-and-setup-details).
1. [Helm](https://helm.sh/docs/intro/install/) is installed
1. Access to download *treeverse/lakefs-enterprise* from [Docker Hub](https://hub.docker.com/u/treeverse). [Contact us](https://lakefs.io/contact-sales/) to gain access to lakeFS Enterprise features.
1. A KV Database. The available options are dependent in your [deployment platform](../../howto/deploy/index.md#deployment-and-setup-details).
1. A method to route traffic into lakeFS from outside of the cluster (via Ingress or Service).

#### Optional

Access to configure your SSO IdP [supported by lakeFS Enterprise][lakefs-sso-enterprise-spec].

!!! info
    You can install lakeFS Enterprise without configuring SSO and still benefit from all other lakeFS Enterprise features.

---

### lakeFS Enterprise License

**Using lakeFS Enterprise requires a valid license.** This license is a JSON Web Token (JWT) that provides access to the 
lakeFS Enterprise server and paid features. A license is tied to a specific installation via an installation ID, and to 
a specific organization via an organization ID.

#### Licensed Features
The following lakeFS Enterprise features must be included in the lakeFS Enterprise license in order to be available:

##### Authentication & Authorization

- [SSO (Single Sign-On)](../../security/sso.md)
- [RBAC (Role-Based Access Control)](../../security/rbac.md)
- [SCIM (System for Cross-domain Identity Management)](../../howto/scim.md)
- [IAM (Identity and Access Management) Role Authentication](../../security/external-principals-aws.md)

##### Advanced Functionality

- [Mount](../../reference/mount.md)
- [Metadata Search](https://info.lakefs.io/metadata-search)
- [MSB (Multiple Storage Backends)](../../howto/multiple-storage-backends.md)
- [Transactional Mirroring](../../howto/mirroring.md)
- [Sparkless GC (Garbage Collection)](../../howto/garbage-collection/standalone-gc.md)

!!! warning 
    Without a license for these features, they will be disabled, and any attempt to access them will result in a 'feature not licensed' error.

#### License Configuration in lakeFS Enterprise

1. <u>Contact Support for a License</u>  
📧 Email: [support@treeverse.io](mailto:support@treeverse.io)

2. <u>Receive Your License Token</u>  
You will receive a license token that contains:  
    - Organization ID  
    - Installation ID  
    - Issue date  
    - Expiry date  
    - Enabled features  
    - Feature limitations  

3. <u>Configure lakeFS Enterprise Server</u>  
    1. Save the license token to a file.  
    2. Provide the file path in the lakeFS Enterprise configuration file:
      ```yaml
      license:
        path: "/path/to/your/license.txt"
      ```

    !!! tip
        Instead of configuring the license token path via the lakeFS Enterprise configuration file, you can set it via the environment variable: `LAKEFS_LICENSE_PATH`.

??? tip "Reading a Currently Installed License Token via API"    
    **Request:**
    ```bash
    GET https://your-lakefs-server/api/v1/license
    ```

    **Response:**
    ```json
    {
      "token": "eyJhbGciOiJSUzI1NiIs..."
    }
    ```

    The returned token can be decoded using any JWT decoding tool to view the license information.

#### License Monitoring & Updates
##### Updating Your License

1. Replace the content of your license file with the new license token.  
2. lakeFS Enterprise will automatically detect and reload the new license within 1 minute.  

!!! warning
    You cannot change the file path itself to point to a new file while the server is running.

##### Automatic Monitoring

- <u>Validation and Expiry Check:</u> Occurs periodically to verify license validity and expiry.
- <u>File Monitoring:</u> The server checks periodically to detect license file changes so that when the license token gets updated in the file, there is no need to restart the server and the license updates automatically.

#### Troubleshooting
##### Server Won't Start
```
Error: no valid license found for this lakeFS Enterprise Server
```
<u>Solution:</u> Ensure you have configured the `license.path` in the lakeFS Enterprise configuration file or environment variables.

---

##### License Signature Verification Failed
```
Error: token signature did not match any known public key: token signature is invalid: crypto/rsa
```
<u>Solutions:</u>  
    1. Re-download your license token from the original source.  
    2. Verify the license file contains only the license token with no extra characters or line breaks.  
    3. Contact [support@treeverse.io](mailto:support@treeverse.io) to get a valid license token.  

---

##### License Has Expired
```
Error: license has expired. Please contact support immediately: invalid license found
```
```
Error: license expired: invalid expiry date
```
<u>Solutions:</u>    
    1. If recently renewed, ensure you've updated the license file with the new token.  
    2. Contact [support@treeverse.io](mailto:support@treeverse.io) immediately for license renewal.  

---

##### License File Not Found
```
Error: open /path/to/license/file/you/provided: no such file or directory
```
<u>Solutions:</u>  
    1. Verify the exact path and filename in your `license.path` configuration.  
    2. Check for typos in filename.  
    3. Check that license file wasn't moved or removed after configuration.  
    4. Check that the license file exists at the specified location.  
    5. Ensure the file extension is included.  
    6. Use absolute paths instead of relative paths.  

---

##### License File Permission Denied
```
Error: open /path/to/license/file/you/provided: permission denied
```
<u>Solutions:</u>  
    1. Check file permissions and set appropriate permissions.  
    2. Ensure lakeFS Enterprise process has permission to read the file and access parent directories.  

---

##### Installation ID Mismatch
```
Error: license belongs to installation ID X (current installation ID: Y)
```
<u>Solutions:</u>  
    1. Check if you have the correct license file for this specific installation.  
    2. Check that your installation ID didn't change.  
    3. Contact [support@treeverse.io](mailto:support@treeverse.io) to get a correct new license.  

---

##### Malformed or Empty License Token
```
Error: parsing token: token is malformed: token contains an invalid number of segments
```
```
Error: license has invalid expiry (no license?): invalid license found
```
<u>Solutions:</u>  
    1. Verify the license file contains the complete license token.  
    2. Remove any extra whitespace, newlines, or characters from the license file.  
    3. Re-copy the license token from the original source.  
    4. Ensure the file contains only the license token and nothing else.  

---

##### Feature Not Available
```
Error: feature not licensed - to enable, contact support@treeverse.io
```
<u>Solution:</u> Your current license doesn't include this feature. Contact [support@treeverse.io](mailto:support@treeverse.io) to upgrade your license.

!!! tip "Need Help?"
    If you encounter an issue not covered here, contact our support team at [support@treeverse.io](mailto:support@treeverse.io).
---

### Add the lakeFS Helm Chart

* Add the lakeFS Helm repository with `helm repo add lakefs https://charts.lakefs.io`
* The chart contains a values.yaml file you can customize to suit your needs as you follow this guide. Use `helm show values lakefs/lakefs` to see the default values.
* Configure `image.privateRegistry.secretToken` with the Docker Hub token you received.

---

### License Configuration

!!! note 
    You can proceed without this step at the moment. However, licensing enforcement will be introduced soon. 
    Contact [support](mailto:support@treeverse.io) to receive your installation license.

lakeFS Enterprise requires a valid license to work.
In the helm chart the license is provided as a JWT token either from an existing secret or explicitly.
The following demonstrates how to configure the license in the `values` file:

=== "License with token provided in secrets"

    ```yaml
    enterprise:
      enabled: true

    secrets:
        licenseContents: <Your licese JWT token>
    ```

=== "License with token provided from existing secret"

    ```yaml
    enterprise:
      enabled: true

    # Name of existing secret to use
    existingSecret: <Name of existing secret>

    secretKeys:
      # Use to fetch license token from an existing secret:
       licenseContentsKey: <Name of license contents key from existing secret>
    ```


### Authentication Configuration

Authentication in lakeFS Enterprise is handled directly by the lakeFS Enterprise service. This section explains the configurations required for setting up SSO.

See [SSO for lakeFS Enterprise][lakefs-sso-enterprise-spec] for the supported identity providers and protocols.

The examples below include example configuration for each of the supported SSO protocols. Note the IdP-specific details you'll need to replace with your IdP details.

=== "OpenID Connect"

    The following `values` file will run lakeFS Enterprise with OIDC integration.

    !!! tip
        The full OIDC configurations explained [here][lakefs-sso-enterprise-spec-oidc].

    ```yaml
    enterprise:
      enabled: true
      auth:
        oidc:
          enabled: true
          # secret given by the OIDC provider (e.g auth0, Okta, etc)
          client_secret: <oidc-client-secret>

    image:
      privateRegistry:
        enabled: true
        secretToken: <dockerhub-token>

    lakefsConfig: |
      logging:
        level: "INFO"
      blockstore:
        type: s3
      auth:
        logout_redirect_url: https://oidc-provider-url.com/logout/example
        oidc:
          # the claim that's provided by the OIDC provider (e.g Okta) that will be used as the username according to OIDC provider claims provided after successful authentication
          friendly_name_claim_name: "<some-oidc-provider-claim-name>"
          default_initial_groups: ["Developers", "Admins"]
          # if true then the value of friendly_name_claim_name will be refreshed during each login to maintain the latest value
          # and the the claim value (i.e user name) will be stored in the lakeFS database
          persist_friendly_name: true
        providers:
          oidc:
            post_login_redirect_url: /
            url: https://oidc-provider-url.com/ 
            client_id: <oidc-client-id>         
            callback_base_url: https://<lakefs.acme.com>
            # the claim name that represents the client identifier in the OIDC provider (e.g Okta)
            logout_client_id_query_parameter: client_id
            # the query parameters that will be used to redirect the user to the OIDC provider after logout
            logout_endpoint_query_parameters:
              - returnTo
              - https://<lakefs.acme.com>/oidc/login

    ingress:
      enabled: true
      ingressClassName: <class-name>
      hosts:
        - host: <lakefs.acme.com>
          paths:
            - /
    ```

=== "SAML (With Azure AD)"

    The following `values` file will run lakeFS Enterprise with SAML using Azure AD as the IdP.

    You can use this example configuration to configure Active Directory Federation Services (AD FS) with SAML.

    !!! tip
        The full SAML configurations explained [here][lakefs-sso-enterprise-spec-saml].

    #### Azure App Configuration

    1. Create an Enterprise Application with SAML toolkit - see [Azure quickstart](https://learn.microsoft.com/en-us/entra/identity/enterprise-apps/add-application-portal)
    1. Add users: **App > Users and groups**: Attach users and roles from their existing AD users
      list - only attached users will be able to login to lakeFS.
    1. Configure SAML: App >  Single sign-on > SAML:
      1. Entity ID: Add 2 ID's, lakefs-url + lakefs-url/saml/metadata (e.g. https://lakefs.acme.com and https://lakefs.acme.com/saml/metadata)
      1. Reply URL: lakefs-url/saml (e.g. https://lakefs.acme.com/saml)
      1. Sign on URL: lakefs-url/sso/login-saml (e.g. https://lakefs.acme.com/sso/login-saml)
      1. Relay State (Optional, controls where to redirect after login): /

    #### SAML Configuration

    1. Configure SAML application in your IdP (i.e Azure AD) and replace the required parameters into the `values.yaml` below.
    2. To generate certificates keypair use: `openssl req -x509 -newkey rsa:2048 -keyout myservice.key -out myservice.cert -days 365 -nodes -subj "/CN=lakefs.acme.com"`

    ```yaml
    enterprise:
      enabled: true
      auth:
        saml:
          enabled: true
          createCertificateSecret: true  # NEW: Auto-creates secret
          certificate:
            # certificate and private key for the SAML service provider to sign outgoing SAML requests
            samlRsaPublicCert: |          # RENAMED: from saml_rsa_public_cert
              -----BEGIN CERTIFICATE-----
              ...
              -----END CERTIFICATE-----
            samlRsaPrivateKey: |          # RENAMED: from saml_rsa_private_key
              -----BEGIN PRIVATE KEY-----
              ...
              -----END PRIVATE KEY-----

    secrets:
      authEncryptSecretKey: "some random secret string"

    image:
      privateRegistry:
        enabled: true
        secretToken: <dockerhub-token>

    lakefsConfig: |
      logging:
        level: "DEBUG"
      blockstore:
        type: local
      auth:
        logout_redirect_url: https://<lakefs.acme.com>
        cookie_auth_verification:
          auth_source: saml
          # claim name to use for friendly name in lakeFS UI
          friendly_name_claim_name: displayName
          external_user_id_claim_name: samName
          default_initial_groups:
            - "Developers"
        providers:
          saml:
            post_login_redirect_url: https://<lakefs.acme.com>
            sp_root_url: https://<lakefs.acme.com>
            sp_sign_request: false 
            sp_signature_method: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
            idp_metadata_url: "https://<adfs-auth.company.com>/federationmetadata/2007-06/federationmetadata.xml" 
            # the default id format urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified
            # idp_authn_name_id_format: "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
            idp_skip_verify_tls_cert: true

    ingress:
      enabled: true
      ingressClassName: <class-name>
      annotations: {}
      hosts:
        - host: <lakefs.acme.com>
          paths:
            - /
    ```

=== "LDAP"

    The following `values` file will run lakeFS Enterprise with LDAP.

    !!! tip
        The full LDAP configurations explained [here][lakefs-sso-enterprise-spec-ldap].

    ```yaml
    enterprise:
      enabled: true
      auth:
        ldap:
          enabled: true
          bindPassword: <ldap bind password>

    image:
      privateRegistry:
        enabled: true
        secretToken: <dockerhub-token>

    lakefsConfig: |
      logging:
        level: "INFO"
      blockstore:
        type: local
      auth:
        ui_config:
          login_url: /auth/login
          logout_url: /logout
          login_cookie_names:
            - internal_auth_session
        providers:
          ldap:
            server_endpoint: 'ldaps://ldap.company.com:636'
            bind_dn: uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com
            username_attribute: uid
            user_base_dn: ou=Users,o=<org-id>,dc=<company>,dc=com
            user_filter: (objectClass=inetOrgPerson)
            connection_timeout_seconds: 15
            request_timeout_seconds: 17
            # RBAC group for first time users
            default_user_group: "Developers"

    ingress:
      enabled: true
      ingressClassName: <class-name>
      hosts:
        - host: <lakefs.acme.com>
          paths:
            - /

    ```

See [additional examples on GitHub](https://github.com/treeverse/charts/tree/master/examples/lakefs/enterprise) we provide for each authentication method (oidc, saml, ldap, rbac, external AWS IAM).

---

### Database Configuration

In this section, you will learn how to configure lakeFS Enterprise to work with the KV Database you created (see [prerequisites](#prerequisites)).

Notes:

* By default, the lakeFS Helm chart comes with `useDevPostgres: false`, you can change it to `useDevPostgres: true` for dev use. This setup is useful when you want to run a setup with multiple replicas or want to prevent data loss between containers restarts.
* See [lakeFS database configuration](../../reference/configuration.md#database).

The database configuration can be set directly via `lakefsConfig`, via K8S Secret Kind, or via environment variables.

=== "Postgres via environment variables"

    This example uses Postgres as KV Database configured via environment variables.

    ```yaml 
    extraEnvVars:
      - name: LAKEFS_DATABASE_TYPE
        value: postgres
      - name: LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING
        value: '<postgres connection string>'
    ```

=== "Via lakefsConfig"

    This example uses DynamoDB as KV Database.

    ```yaml
    lakefsConfig: |
      database:
        type: dynamodb
        dynamodb:
          table_name: <table>
          aws_profile: <profile>
          aws_region: <region>
    ```

=== "Postgres via shared Secret kind"

    This example uses Postgres as KV Database. The chart will create a `kind: Secret` holding the database connection string.

    ```yaml
    secrets:
      authEncryptSecretKey: shared-key-hello
      databaseConnectionString: <postgres connection string>

    lakefsConfig: |
      database:
        type: postgres
    ```

---

### Install the lakeFS Helm Chart

After populating your values.yaml file with the relevant configuration, in the desired K8S namespace run `helm install lakefs lakefs/lakefs -f values.yaml`

---

### Access the lakeFS UI

In your browser, go to the Ingress host to access lakeFS UI.

## Log Collection

The recommended practice for collecting logs would be sending them to the container std (default configuration)
and letting an external service to collect them to a sink. An example for logs collector would be [fluentbit](https://fluentbit.io/)
that can collect container logs, format them and ship them to a target like S3.

There are 2 kinds of logs:
- Regular logs like an API error or some event description used for debugging
- Audit logs that describe user actions (i.e create branch)

The distinction between regular logs and audit_logs is in the boolean field `log_audit`.

## Advanced Deployment Configurations

The following example demonstrates a scenario where you need to configure an HTTP proxy for lakeFS, TLS certificates for the Ingress and extending the K8S manifests without forking the Helm chart.

```yaml
ingress:
  enabled: true
  ingressClassName: <class-name>
  # configure TLS certificate for the Ingress
  tls:
    - hosts:
      - lakefs.acme.com
      secretName: somesecret
  hosts:
    - host: lakefs.acme.com
      paths:
       - /

# configure proxy for lakeFS
extraEnvVars:
  - name: HTTP_PROXY
    value: 'http://my.company.proxy:8081'
  - name: HTTPS_PROXY
    value: 'http://my.company.proxy:8081'

# advanced: extra manifests to extend the K8S resources
extraManifests:
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: '{% raw %}{{ .Values.lakefs.name }}{% endraw %}-extra-config'
    data:
      config.yaml: my-data
```

[lakefs-sso-enterprise-spec]: ../../security/sso.md#sso-for-lakefs-enterprise
[lakefs-sso-enterprise-spec-oidc]: ../../security/sso.md#openid-connect
[lakefs-sso-enterprise-spec-saml]: ../../security/sso.md#active-directory-federation-services-ad-fs-using-saml
[lakefs-sso-enterprise-spec-ldap]: ../../security/sso.md#ldap
[lakefs-enterprise-architecture]: ../../understand/architecture.md
[lakefs-enterprise-upgrade]: ../../enterprise/upgrade.md