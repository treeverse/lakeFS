---
title: Install
description: lakeFS Enterprise Installation Guide
parent: Get Started
grand_parent: lakeFS Enterprise
nav_order: 202
---

# Install


## lakeFS Helm Chart

In order to use lakeFS Enterprise and Fluffy, we provided out of the box setup, see [lakeFS Helm chart configuration](https://github.com/treeverse/charts/tree/master/charts/lakefs).

```bash
# Add the lakeFS repository
helm repo add lakefs https://charts.lakefs.io
# Deploy lakeFS
helm install <release-name> lakefs/lakefs -f <values.yaml>
```

Notes:
* By default the chart is deployed with a Postgres pod for quick-start, make sure to replace that to a stable database by setting `useDevPostgres: false` in the chart values.
* The encrypt secret key `secrets.authEncryptSecretKey` is shared between fluffy and lakeFS for authentication.
* Fluffy docker image: replace the `fluffy.image.privateRegistry.secretToken` with the token you recieved to dockerhub to fetch the fluffy docker image.
* Check the [additional examples on GitHub](https://github.com/treeverse/charts/tree/master/examples/lakefs/enterprise) we provide for each authentication method (oidc, adfs, ldap, rbac, IAM etc).
* The Database configurations between fluffy and lakeFS should be the same since they connect to the same DB.


## Deploy lakeFS Enterprise (Kubernetes)


The following examples will guide you through the installation of lakeFS Enterprise using our [Helm Chart](#lakefs-helm-chart).

### Prerequisites

1. A KV Database, like postgres, should be configured and shared by fluffy and lakeFS.
1. Access to configure SSO IdP, like Azure AD Enterprise Application.
1. A proxy server should be configured to route traffic between the 2 servers (in K8S that is Ingress).
1. Token for [dockerhub/fluffy](https://hub.docker.com/u/treeverse) Docker image. Please [contact us](mailto:support@treeverse.io) to get access to the Dockerhub image.

### Deploy lakeFS Enterprise with SSO


Authentication in lakeFS Enterprise is handled by the Fluffy sso service which runs side-by-side with lakeFS.
The following examples are based on our [Helm Chart](#lakefs-helm-chart).

For details on configuring the supported identity providers with Fluffy in-depth configuration see [SSO][lakefs-sso-enterprise-spec].

**Note:** Full Fluffy configuration can be found [here][fluffy-configuration].
{: .note }

* OpenID Connect
* SAML
* Active Directory Federation Services (AD FS) (using SAML)
* LDAP

If you're using an authentication provider that is not listed please [contact us](support@treeverse.io) for further assistance.


<div class="tabs">
  <ul>
    <li><a href="#oidc">OpenID Connect</a></li>
    <li><a href="#saml">SAML (Azure AD)</a></li>
    <li><a href="#ldap">LDAP</a></li>
  </ul>
  <div markdown="1" id="oidc">
### OpenID Connect

As an example, the following `values` file will run lakeFS Enterprise with OIDC integration.


**Note:** Full OIDC configurations explained [here][lakefs-sso-enterprise-spec-oidc].
{: .note }

1. Create `values.yaml` file and replace the placeholders with your OIDC provider details, token and ingress host.
2. Run `helm install lakefs lakefs/lakefs -f values.yaml` in the desired K8S namespace.

```yaml
lakefsConfig: |
  logging:
      level: "INFO"
  blockstore:
    type: s3
  auth:
    oidc:
      # the claim that's provided by the OIDC provider (e.g Okta) that will be used as the username according to OIDC provider claims provided after successful authentication
      friendly_name_claim_name: "<some-oidc-provider-claim-name>"
      default_initial_groups: ["Developers", "Admins"]
      # if true then the value of friendly_name_claim_name will be refreshed during each login to maintain the latest value
      # and the the claim value (i.e user name) will be stored in the lakeFS database
      persist_friendly_name: true
    ui_config:
      login_cookie_names:
        - internal_auth_session
        - oidc_auth_session
ingress:
  enabled: true
  ingressClassName: <class-name>
  hosts:
    # the ingress that will be created for lakeFS
    - host: <lakefs.acme.com>
      paths:
       - /

##################################################
########### lakeFS enterprise - FLUFFY ###########
##################################################

fluffy:
  enabled: true
  image:
    repository: treeverse/fluffy
    pullPolicy: IfNotPresent
    privateRegistry:
      enabled: true
      secretToken: <dockerhub-token-fluffy-image>
  fluffyConfig: |
    logging:
      format: "json"
      level: "INFO"
    auth:
      logout_redirect_url: https://oidc-provider-url.com/logout/example
      oidc:
        enabled: true
        url: https://oidc-provider-url.com/
        client_id: <oidc-client-id>
        callback_base_url: https://<lakefs.acme.com>
        # the claim name that represents the client identifier in the OIDC provider (e.g Okta)
        logout_client_id_query_parameter: client_id
        # the query parameters that will be used to redirect the user to the OIDC provider (e.g Okta) after logout
        logout_endpoint_query_parameters:
          - returnTo
          - https://<lakefs.acme.com>/oidc/login
  secrets:
    create: true
  sso:
    enabled: true
    oidc:
      enabled: true
      # secret given by the OIDC provider (e.g auth0, Okta, etc) store in kind: Secret
      client_secret: <oidc-client-secret>
  rbac:
    enabled: true

useDevPostgres: true
```

  </div>
  <div markdown="1" id="saml">
### SAML (Azure AD)

The following example will walk you through the deployment of lakeFS Enterprise with SAML integration using Azure AD as the IDP.
The following example uses SAML, a common setup, although Azure Entra also supports OIDC.


**Note:** Active Directory Federation Services (AD FS) can be configured the same using SAML.
{: .note }

### Azure App Configuration

1. Create an Enterprise Application with SAML toolkit - see [Azure quickstart](https://learn.microsoft.com/en-us/entra/identity/enterprise-apps/add-application-portal)
1. Add users: **App > Users and groups**: Attach users and roles from their existing AD users
   list - only attached users will be able to login to lakeFS.
1. Configure SAML: App >  Single sign-on > SAML:
   1. Entity ID: Add 2 ID’s, lakefs-url + lakefs-url/saml/metadata (e.g. https://lakefs.acme.com)
   1. Reply URL: lakefs-url/saml (e.g. https://lakefs.acme.com/saml)
   1. Sign on URL: lakefs-url/sso/login-saml (e.g. https://lakefs.acme.com/sso/login-saml)
   1. Relay State (Optional): /

### Deploy lakeFS Chart

**Note:** Full SAML configurations explained [here][lakefs-sso-enterprise-spec-saml].
{: .note }

1. Configure SAML application in your IDP (i.e Azure AD) and replace the required parameters into the `values.yaml` below.
2. To generate certificates keypair use: `openssl req -x509 -newkey rsa:2048 -keyout myservice.key -out myservice.cert -days 365 -nodes -subj "/CN=lakefs.acme.com" -
3. Run `helm install lakefs lakefs/lakefs -f values.yaml` in the desired K8S namespace.
4. In your browser go to [https://lakefs.acme.com](https://lakefs.acme.com) to access lakeFS UI.

```yaml
secrets:
  authEncryptSecretKey: "some random secret string"

lakefsConfig: |
  logging:
      level: "DEBUG"
  blockstore:
    type: local
  auth:
    cookie_auth_verification:
      # claim name to use for friendly name in lakeFS UI
      friendly_name_claim_name: displayName
      external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name
      default_initial_groups:
        - "Developers"
    encrypt:
      secret_key: shared-secrey-key
    ui_config:
      login_cookie_names:
        - internal_auth_session
        - saml_auth_session
ingress:
  enabled: true
  ingressClassName: <class-name>
  annotations: {}
  hosts:
    - host: <lakefs.acme.com>
      paths:
       - /

fluffy:
  enabled: true
  image:
    repository: treeverse/fluffy
    pullPolicy: IfNotPresent
    privateRegistry:
      enabled: true
      secretToken: <dockerhub-token-fluffy-image>
  fluffyConfig: |
    logging:
      format: "json"
      level: "DEBUG"
    auth:
      # redirect after logout
      logout_redirect_url: https://<lakefs.acme.com>
      saml:
        sp_sign_request: false
        sp_signature_method: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
        idp_metadata_url: https://login.microsoftonline.com/<...>/federationmetadata/2007-06/federationmetadata.xml?appid=<app-id>
        # the default id format urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified
        # idp_authn_name_id_format: "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
        external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name
        idp_skip_verify_tls_cert: true
  secrets:
    create: true
  sso:
    enabled: true
    saml:
      enabled: true
      createSecret: true
      lakeFSServiceProviderIngress: https://<lakefs.acme.com>
      certificate:
        # certificate and private key for the SAML service provider to sign outgoing SAML requests
        saml_rsa_public_cert: |
          -----BEGIN CERTIFICATE-----
          ...
          -----END CERTIFICATE-----
        saml_rsa_private_key: |
          -----BEGIN PRIVATE KEY-----
          ...
          -----END PRIVATE KEY-----
  rbac:
    enabled: true
```

  </div>
  <div markdown="1" id="ldap">

### LDAP `values.yaml` file for helm deployments
{:.no_toc}

**Note:** Full LDAP configurations explained [here][lakefs-sso-enterprise-spec-ldap].
{: .note }

1. Create `values.yaml` file and replace the placeholders with your LDAP provider details, token and ingress host.
2. Run `helm install lakefs lakefs/lakefs -f values.yaml` in the desired K8S namespace.
3. In your browser go to [https://lakefs.acme.com](https://lakefs.acme.com) to access lakeFS UI with a valid LDAP user name and password.

```yaml
lakefsConfig: |
  logging:
      level: "INFO"
  blockstore:
    type: local
  auth:
    remote_authenticator:
      enabled: true
      # RBAC group for first time users
      default_user_group: "Developers"
    ui_config:
      login_cookie_names:
        - internal_auth_session

ingress:
  enabled: true
  ingressClassName: <class-name>
  hosts:
    - host: <lakefs.acme.com>
      paths:
       - /

fluffy:
  enabled: true
  image:
    privateRegistry:
      enabled: true
      secretToken: <dockerhub-token-fluffy-image>
  fluffyConfig: |
    logging:
      level: "INFO"
    auth:
      post_login_redirect_url: /
      ldap:
        server_endpoint: 'ldaps://ldap.company.com:636'
        bind_dn: uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com
        username_attribute: uid
        user_base_dn: ou=Users,o=<org-id>,dc=<company>,dc=com
        user_filter: (objectClass=inetOrgPerson)
        connection_timeout_seconds: 15
        request_timeout_seconds: 17
  secrets:
    create: true
  sso:
    enabled: true
    ldap:
      enabled: true
      bind_password: <ldap bind password>
  rbac:
    enabled: true

useDevPostgres: true
```

  </div>
</div>

### Configuring lakeFS KV store in the Helm Chart

The lakeFS Helm chart supports multiple ways of configurintg the database that is the KV Store (DynamoDB, Postgres etc), the default is a dev Postgres container (set `useDevPostgres: false` to disable).
The configuration structure used for the KV store is the same for both lakeFS and Fluffy.
See [fluffy][fluffy-configuration] and [lakeFS]({% link reference/configuration.md %}#database) `database` configuration.

Essentially the database configuration structure between lakeFS and fluffy can be set via K8S Secret Kind, directly via `fluffyConfig` and `lakefsConfig` or via environment variables.


<div class="tabs">
  <ul>
    <li><a href="#dynamodb-via-config">DynamoDB via config</a></li>
    <li><a href="#postgres-via-secret-kind">Postgres via shared Secret kind</a></li>
    <li><a href="#postgres-via-env-vars">Postgres via envrionment varialbles</a></li>
  </ul>
<div markdown="1" id="dynamodb-via-config">

#### DynamoDB via config (AWS)

```yaml

# disable dev postgres
useDevPostgres: false

lakefsConfig: |
  database:
    type: dynamodb
    dynamodb:
      table_name: <table>
      aws_profile: <profile>
fluffyConfig: |
    database:
      type: dynamodb
      dynamodb:
        table_name: <table>
        aws_profile: <profile>
        aws_region: <region>
```
</div>
<div markdown="1" id="postgres-via-secret-kind">


#### Postgres via kind: Secret

The chart will create a `kind: Secret` holding the database connection string, and the lakeFS and Fluffy will use it.

```yaml
useDevPostgres: false
secrets:
  authEncryptSecretKey: shared-key-hello
  databaseConnectionString: <postgres connection string>

lakefsConfig: |
  database:
    type: postgres
fluffyConfig: |
  database:
    type: postgres
```
</div>

<div markdown="1" id="postgres-via-env-vars">

#### Postgres via environment variables

lakeFS is configured via `lakefsConfig` and Fluffy via environment with the same database configuration.

```yaml
useDevPostgres: false
lakefsConfig: |
  database:
    type: postgres
    postgres:
      connection_string: <postgres connection string>

fluffy:
  extraEnvVars:
    - name: FLUFFY_DATABASE_TYPE
      value: postgres
    - name: FLUFFY_DATABASE_POSTGRES_CONNECTION_STRING
      value: '<postgres connection string>'
```
</div>
</div>

### Handling HTTP proxies, TLS certificates and other configurations (Advanced)

The following example demonstrates a scenario where you need to configure an HTTP proxy for lakeFS and Fluffy, TLS certificates for the Ingress and extending the K8S manifests without forking the Helm chart.

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

fluffy:
  # configure proxy for fluffy
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
      name: '{{ .Values.fluffy.name }}-extra-config'
    data:
      config.yaml: my-data
```

[lakefs-sso-enterprise-spec]: {% link reference/security/sso.md %}#sso-for-lakefs-enterprise
[lakefs-sso-enterprise-spec-oidc]: {% link reference/security/sso.md %}#oidc
[lakefs-sso-enterprise-spec-saml]: {% link reference/security/sso.md %}#adfs
[lakefs-sso-enterprise-spec-ldap]: {% link reference/security/sso.md %}#ldap
[fluffy-configuration]: {% link enterprise/configuration.md %}#fluffy-server-configuration

