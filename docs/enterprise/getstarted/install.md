---
title: Install
description: lakeFS Enterprise Installation Guide
parent: Get Started
grand_parent: lakeFS Enterprise
nav_order: 202
---

# Install

For production deployments of lakeFS Enterprise, follow this guide.

{% include toc_2-3.html %}

## lakeFS Enterprise Architecture

We recommend to review the [lakeFS Enterprise architecture][lakefs-enterprise-architecture] to understand the components you will be deploying.

## Deploy lakeFS Enterprise on Kubernetes

The guide is using the [lakeFS Helm Chart](https://github.com/treeverse/charts/tree/master/charts/lakefs) to deploy a fully functional lakeFS Enterprise.

The guide includes example configurations, follow the steps below and adjust the example configurations according to:
* The platform you run on: among the platform [supported by lakeFS](../../howto/deploy/index.md#deployment-and-setup-details)
* Type of KV store you use
* Your SSO IdP and protocol

### Prerequisites

1. You have a Kubernetes cluster running in one of the platforms [supported by lakeFS](../../howto/deploy/index.md#deployment-and-setup-details).
1. [Helm](https://helm.sh/docs/intro/install/) is installed
1. Access to download *dockerhub/fluffy* from [Docker Hub](https://hub.docker.com/u/treeverse). [Contact us](https://lakefs.io/contact-sales/) to gain access to lakeFS Enterprise features.
1. A KV Database that will be shared by lakeFS and Fluffy. The available options are dependent in your [deployment platform](../../howto/deploy/index.md#deployment-and-setup-details).
1. A proxy server configured to route traffic between the lakeFS and Fluffy servers, see Reverse Proxy in [lakeFS Enterprise architecture][lakefs-enterprise-architecture].

#### Optional
1. Access to configure your SSO IdP [supported by lakeFS Enterprise][lakefs-sso-enterprise-spec].

{: .note }
> You can install lakeFS Enterprise without configuring SSO and still benefit from all other lakeFS Enterprise features.

### Add the lakeFS Helm Chart

* Add the lakeFS Helm repository with `helm repo add lakefs https://charts.lakefs.io`
* The chart contains a values.yaml file you can customize to suit your needs as you follow this guide. Use `helm show values lakefs/lakefs` to see the default values.
* While customizing your values.yaml file, note to configure `fluffy.image.privateRegistry.secretToken` with the token Docker Hub token you received.

### Authentication Configuration

Authentication in lakeFS Enterprise is handled by the Fluffy SSO service which runs side-by-side to lakeFS. This section explains
what Fluffy configurations are required for configuring the SSO service. See [this][fluffy-configuration] configuration reference for additional Fluffy configurations.

See [SSO for lakeFS Enterprise][lakefs-sso-enterprise-spec] for the supported identity providers and protocols.

The examples below include example configuration for each of the supported SSO protocols. Note the IdP-specific details you'll need to
replace with your IdP details.

<div class="tabs">
  <ul>
    <li><a href="#oidc">OpenID Connect</a></li>
    <li><a href="#saml">SAML (with Azure AD)</a></li>
    <li><a href="#ldap">LDAP</a></li>
  </ul>
  <div markdown="1" id="oidc">

The following `values` file will run lakeFS Enterprise with OIDC integration.

{: .note }
> The full OIDC configurations explained [here][lakefs-sso-enterprise-spec-oidc].

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

The following `values` file will run lakeFS Enterprise with SAML using Azure AD as the IdP.
<br>
You can use this example configuration to configure Active Directory Federation Services (AD FS) with SAML.

{: .note }
> The full SAML configurations explained [here][lakefs-sso-enterprise-spec-saml].

#### Azure App Configuration
{: .no_toc}

1. Create an Enterprise Application with SAML toolkit - see [Azure quickstart](https://learn.microsoft.com/en-us/entra/identity/enterprise-apps/add-application-portal)
1. Add users: **App > Users and groups**: Attach users and roles from their existing AD users
   list - only attached users will be able to login to lakeFS.
1. Configure SAML: App >  Single sign-on > SAML:
   1. Entity ID: Add 2 ID’s, lakefs-url + lakefs-url/saml/metadata (e.g. https://lakefs.acme.com and https://lakefs.acme.com/saml/metadata)
   1. Reply URL: lakefs-url/saml (e.g. https://lakefs.acme.com/saml)
   1. Sign on URL: lakefs-url/sso/login-saml (e.g. https://lakefs.acme.com/sso/login-saml)
   1. Relay State (Optional, controls where to redirect after login): /

#### SAML Configuration
{: .no_toc}

1. Configure SAML application in your IdP (i.e Azure AD) and replace the required parameters into the `values.yaml` below.
2. To generate certificates keypair use: `openssl req -x509 -newkey rsa:2048 -keyout myservice.key -out myservice.cert -days 365 -nodes -subj "/CN=lakefs.acme.com" -

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

The following `values` file will run lakeFS Enterprise with LDAP.

> The full LDAP configurations explained [here][lakefs-sso-enterprise-spec-ldap].
{: .note }

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

See [additional examples on GitHub](https://github.com/treeverse/charts/tree/master/examples/lakefs/enterprise) we provide for each authentication method (oidc, adfs, ldap, rbac, IAM etc).

### Database Configuration

In this section, you will learn how to configure lakeFS Enterprise to work with the KV Database you created (see [prerequisites](#prerequisites).

Notes:
* By default, the lakeFS Helm chart comes with `useDevPostgres: true`, you should change it to `useDevPostgres: false` for Fluffy to work with your KV Database and be suitable for production needs.
* The KV database is shared between lakeFS and Fluffy, and therefore both services must use the same configuration.
* See [fluffy][fluffy-configuration] and [lakeFS]({% link reference/configuration.md %}#database) `database` configuration.

The database configuration structure between lakeFS and fluffy can be set directly via `fluffyConfig`, via K8S Secret Kind, and `lakefsConfig` or via environment variables.

<div class="tabs">
  <ul>
    <li><a href="#postgres-via-env-vars">Postgres via environment variables</a></li>
    <li><a href="#via-fluffy-config">Via fluffyConfig</a></li>
    <li><a href="#postgres-via-secret-kind">Postgres via shared Secret kind</a></li>
  </ul>
<div markdown="1" id="postgres-via-env-vars">

This example uses Postgres as KV Database. lakeFS is configured via `lakefsConfig` and Fluffy via environment with the same database configuration.

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

<div markdown="1" id="via-fluffy-config">

This example uses DynamoDB as KV Database.

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


This example uses Postgres as KV Database. The chart will create a `kind: Secret` holding the database connection string, and the lakeFS and Fluffy will use it.

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

### Install the lakeFS Helm Chart

After populating your values.yaml file with the relevant configuration, in the desired K8S namespace run `helm install lakefs lakefs/lakefs -f values.yaml`

### Access the lakeFS UI

In your browser go to the to the Ingress host to access lakeFS UI.

## Log Collection

The recommended practice for collecting logs would be sending them to the container std (default configuration)
and letting an external service to collect them to a sink. An example for logs collector would be [fluentbit](https://fluentbit.io/)
that can collect container logs, format them and ship them to a target like S3.

There are 2 kinds of logs, regular logs like an API error or some event description used for debugging
and audit_logs that are describing a user action (i.e create branch).
The distinction between regular logs and audit_logs is in the boolean field log_audit.
lakeFS and fluffy share the same configuration structure under logging.* section in the config.


## Advanced Deployment Configurations

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

[lakefs-sso-enterprise-spec]: {% link security/sso.md %}#sso-for-lakefs-enterprise
[lakefs-sso-enterprise-spec-oidc]: {% link security/sso.md %}#oidc
[lakefs-sso-enterprise-spec-saml]: {% link security/sso.md %}#adfs
[lakefs-sso-enterprise-spec-ldap]: {% link security/sso.md %}#ldap
[fluffy-configuration]: {% link enterprise/configuration.md %}#fluffy-server-configuration
[lakefs-enterprise-architecture]: {% link enterprise/architecture.md %}

