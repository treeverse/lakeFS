---
title: Run lakeFS Enterprise
description: Start using lakeFS-enterprise
parent: lakeFS Enterprise
---

# Run lakeFS Enterprise

{% include toc_2-3.html %}

## Overview

lakeFS Enterprise solution consists of 2 main components:
1. lakeFS - Open Source: [treeverse/lakeFS](https://hub.docker.com/r/treeverse/lakefs),
   release info found in [Github releases](https://github.com/treeverse/lakeFS/releases).
2. Fluffy - Proprietary: Includes Enterprise features. Please [contact support](mailto:support@treeverse.io) to obtain a token for retrieving its image from [Treeverse Dockerhub](https://hub.docker.com/u/treeverse). 

## Quickstart with Docker Compose

### Prerequisites

1. Access to download [dockerhub/fluffy](https://hub.docker.com/u/treeverse) Docker image, to login locally `docker login -u <USERNAME> -p <TOKEN>`. Please [contact us](mailto:support@treeverse.io) to get access to the Dockerhub image.
2. [Docker Compose](https://docs.docker.com/compose/install/) installed version `2.23.1` or higher on your machine.

The following docker-compose files will spin up lakeFS, Fluffy and postgres as a shared KV database. 
We provide two docker compose examples. 
The first example (without SSO) is recommended for an easy start and the second example uses OIDC as the SSO authentication method.

⚠️ Using a local postgres is not suitable for production use-cases.

<div class="tabs">
  <ul>
    <li><a href="#docker-compose-no-sso">No SSO
    </a></li>
    <li><a href="#docker-compose-with-sso">With SSO (OIDC)</a></li>
  </ul> 
<div markdown="1" id="docker-compose-no-sso">

### Docker Compose without SSO

For simplicity the example does not use SSO and only supports basic authentication of access key and secret key.
    
1. Create `docker-compose.yaml` file with the following content
2. Run `docker compose up` in the same directory as the `docker-compose.yaml` file.
3. In your browser go to <http://localhost:8080> to access lakeFS UI.

```yaml
version: "3"
services:
  lakefs:
    image: "treeverse/lakefs:1.25.0"
    command: "RUN"
    ports:
      - "8080:8080"
    depends_on:
      - "postgres"
    environment:
      - LAKEFS_LISTEN_ADDRESS=0.0.0.0:8080
      - LAKEFS_LOGGING_LEVEL=DEBUG
      - LAKEFS_AUTH_ENCRYPT_SECRET_KEY="random_secret"
      - LAKEFS_AUTH_API_ENDPOINT=http://fluffy:9000/api/v1
      - LAKEFS_AUTH_API_SUPPORTS_INVITES=true
      - LAKEFS_AUTH_UI_CONFIG_RBAC=internal
      - LAKEFS_AUTH_AUTHENTICATION_API_ENDPOINT=http://localhost:8000/api/v1
      - LAKEFS_AUTH_AUTHENTICATION_API_EXTERNAL_PRINCIPALS_ENABLED=true
      - LAKEFS_DATABASE_TYPE=postgres
      - LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=postgres://lakefs:lakefs@postgres/postgres?sslmode=disable
      - LAKEFS_BLOCKSTORE_TYPE=local
      - LAKEFS_BLOCKSTORE_LOCAL_PATH=/home/lakefs
      - LAKEFS_BLOCKSTORE_LOCAL_IMPORT_ENABLED=true
    entrypoint: ["/app/wait-for", "postgres:5432", "--", "/app/lakefs", "run"]
    configs:
      - source: lakefs.yaml
        target: /etc/lakefs/config.yaml
  postgres:
    image: "postgres:11"
    ports:
      - "5433:5432"
    environment:
      POSTGRES_USER: lakefs
      POSTGRES_PASSWORD: lakefs

  fluffy:
    image: "${FLUFFY_REPO:-treeverse}/fluffy:${TAG:-0.4.4}"
    command: "${COMMAND:-run}"
    ports:
      - "8000:8000"
      - "9000:9000"
    depends_on:
      - "postgres"
    environment:
      - FLUFFY_LOGGING_LEVEL=DEBUG
      - FLUFFY_DATABASE_TYPE=postgres
      - FLUFFY_DATABASE_POSTGRES_CONNECTION_STRING=postgres://lakefs:lakefs@postgres/postgres?sslmode=disable
      - FLUFFY_AUTH_ENCRYPT_SECRET_KEY="random_secret"
      - FLUFFY_AUTH_SERVE_LISTEN_ADDRESS=0.0.0.0:9000
      - FLUFFY_LISTEN_ADDRESS=0.0.0.0:8000
      - FLUFFY_AUTH_SERVE_DISABLE_AUTHENTICATION=true
      - FLUFFY_AUTH_POST_LOGIN_REDIRECT_URL=http://localhost:8080/
    entrypoint: [ "/app/wait-for", "postgres:5432", "--", "/app/fluffy" ]

configs:
  lakefs.yaml:
    content: |
      auth:
        ui_config:
          login_cookie_names:
            - internal_auth_session
```

</div>
<div markdown="1" id="docker-compose-with-sso">

### Docker Compose with SSO (OIDC)

This setup uses OIDC as the SSO authentication method thus requiring a valid OIDC configuration.

Create a `.env` file in the same directory as the `docker-compose.yaml` with the required configurations, docker compose will automatically use that. 

```
FLUFFY_AUTH_OIDC_CLIENT_ID=
FLUFFY_AUTH_OIDC_CLIENT_SECRET=
# The name of the query parameter that is used to pass the client ID to the logout endpoint of the SSO provider, i.e client_id
FLUFFY_AUTH_OIDC_LOGOUT_CLIENT_ID_QUERY_PARAMETER=
FLUFFY_AUTH_OIDC_URL=https://my-sso.com/
FLUFFY_AUTH_LOGOUT_REDIRECT_URL=https://my-sso.com/logout
# Optional: display a friendly name in the lakeFS UI by specifying which claim from the provider to show (i.e name, nickname, email etc)
LAKEFS_AUTH_OIDC_FRIENDLY_NAME_CLAIM_NAME=
```

Next, create a `docker-compose.yaml` file with the following content.

```yaml
version: "3"
services:
  lakefs:
    image: "treeverse/lakefs:1.25.0"
    command: "RUN"
    ports:
      - "8080:8080"
    depends_on:
      - "postgres"
    environment:
      - LAKEFS_LISTEN_ADDRESS=0.0.0.0:8080
      - LAKEFS_LOGGING_LEVEL=DEBUG
      - LAKEFS_AUTH_ENCRYPT_SECRET_KEY="random_secret"
      - LAKEFS_AUTH_API_ENDPOINT=http://fluffy:9000/api/v1
      - LAKEFS_AUTH_API_SUPPORTS_INVITES=true
      - LAKEFS_AUTH_UI_CONFIG_LOGIN_URL=http://localhost:8000/oidc/login
      - LAKEFS_AUTH_UI_CONFIG_LOGOUT_URL=http://localhost:8000/oidc/logout
      - LAKEFS_AUTH_UI_CONFIG_RBAC=internal
      - LAKEFS_AUTH_AUTHENTICATION_API_ENDPOINT=http://localhost:8000/api/v1
      - LAKEFS_AUTH_AUTHENTICATION_API_EXTERNAL_PRINCIPALS_ENABLED=true
      - LAKEFS_DATABASE_TYPE=postgres
      - LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=postgres://lakefs:lakefs@postgres/postgres?sslmode=disable
      - LAKEFS_BLOCKSTORE_TYPE=local
      - LAKEFS_BLOCKSTORE_LOCAL_PATH=/home/lakefs
      - LAKEFS_BLOCKSTORE_LOCAL_IMPORT_ENABLED=true
      - LAKEFS_AUTH_OIDC_FRIENDLY_NAME_CLAIM_NAME=${LAKEFS_AUTH_OIDC_FRIENDLY_NAME_CLAIM_NAME}
    entrypoint: ["/app/wait-for", "postgres:5432", "--", "/app/lakefs", "run"]
    configs:
      - source: lakefs.yaml
        target: /etc/lakefs/config.yaml
  postgres:
    image: "postgres:11"
    ports:
      - "5433:5432"
    environment:
      POSTGRES_USER: lakefs
      POSTGRES_PASSWORD: lakefs

  fluffy:
    image: "${FLUFFY_REPO:-treeverse}/fluffy:${TAG:-0.4.4}"
    command: "${COMMAND:-run}"
    ports:
      - "8000:8000"
      - "9000:9000"
    depends_on:
      - "postgres"
    environment:
      - FLUFFY_LOGGING_LEVEL=DEBUG
      - FLUFFY_DATABASE_TYPE=postgres
      - FLUFFY_DATABASE_POSTGRES_CONNECTION_STRING=postgres://lakefs:lakefs@postgres/postgres?sslmode=disable
      - FLUFFY_AUTH_ENCRYPT_SECRET_KEY="random_secret"
      - FLUFFY_AUTH_SERVE_LISTEN_ADDRESS=0.0.0.0:9000
      - FLUFFY_LISTEN_ADDRESS=0.0.0.0:8000
      - FLUFFY_AUTH_SERVE_DISABLE_AUTHENTICATION=true
      - FLUFFY_AUTH_LOGOUT_REDIRECT_URL=${FLUFFY_AUTH_LOGOUT_REDIRECT_URL}
      - FLUFFY_AUTH_POST_LOGIN_REDIRECT_URL=http://localhost:8080/
      - FLUFFY_AUTH_OIDC_ENABLED=true
      - FLUFFY_AUTH_OIDC_URL=${FLUFFY_AUTH_OIDC_URL}
      - FLUFFY_AUTH_OIDC_CLIENT_ID=${FLUFFY_AUTH_OIDC_CLIENT_ID}
      - FLUFFY_AUTH_OIDC_CLIENT_SECRET=${FLUFFY_AUTH_OIDC_CLIENT_SECRET}
      - FLUFFY_AUTH_OIDC_CALLBACK_BASE_URL=http://localhost:8000
      - FLUFFY_AUTH_OIDC_LOGOUT_CLIENT_ID_QUERY_PARAMETER=${FLUFFY_AUTH_OIDC_LOGOUT_CLIENT_ID_QUERY_PARAMETER}
    entrypoint: [ "/app/wait-for", "postgres:5432", "--", "/app/fluffy" ]
    configs:
      - source: fluffy.yaml
        target: /etc/fluffy/config.yaml

 #This tweak is unfortunate but also necessary. logout_endpoint_query_parameters is a list
 #of strings which isn't parsed nicely as env vars.
configs:
  lakefs.yaml:
    content: |
      auth:
        ui_config:
          login_cookie_names:
            - internal_auth_session
            - oidc_auth_session
        oidc:
          # friendly_name_claim_name: "name"        
          default_initial_groups:
            - Admins

  fluffy.yaml:
    content: |
      auth:
        oidc:
          logout_endpoint_query_parameters:
            - returnTo
            - http://localhost:8080/oidc/login
```

Test the OIDC configuration works - in your browser go to <http://localhost:8080> to access lakeFS UI.

</div>
</div>

For additional examples check out the [lakeFS Enterprise sample](https://github.com/treeverse/lakeFS-samples/tree/main/02_lakefs_enterprise) for all-in-one setup including storage and spark.

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

## Quickstart with the Kubernetes Helm Chart

This examples contains no dependencies and it's the quickest way to start with lakeFS enterprise via Helm on any K8S cluster.
The following values will deploy fluffy and lakeFS without SSO, using local blockstore and a dev postgres container.

1. Create a `values.yaml` file with the following content and make sure to replace `<fluffy-docker-registry-token>`, `<lakefs.acme.com>` and `<ingress-class-name>`.
1. In the desired K8S namespace run `helm install lakefs lakefs/lakefs -f values.yaml`
1. In your browser go to the Ingress host to access lakeFS UI.

```yaml
lakefsConfig: | 
  logging:
      level: "DEBUG"
  blockstore:
    type: local
ingress:
  enabled: true
  ingressClassName: <ingress-class-name>
  annotations: {}
  hosts:
    - host: <lakefs.acme.com>
      paths: 
       - /
fluffy:
  enabled: true
  image:
    privateRegistry:
      enabled: true
      secretToken: <fluffy-docker-registry-token>
  fluffyConfig: |
    logging:
      level: "DEBUG"
  secrets:
    create: true
  sso:
    enabled: false
  rbac:
    enabled: true

# useDevPostgres is true by default and will override any other db configuration, set false for configuring your own db
useDevPostgres: true
```

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

**Note:** Full Fluffy configuration can be found [here]({% link enterprise/fluffy-configuration.md %}).
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
See [fluffy]({% link enterprise/fluffy-configuration.md %}) and [lakeFS]({% link reference/configuration.md %}#database) `database` configuration.

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

## Migrate from lakeFS Open Source to lakeFS Enterprise and upgrades

For upgrading from lakeFS enterprise to a newer version see [lakefs migration](https://docs.lakefs.io/howto/deploy/upgrade.html). 

**To move from lakeFS Open Source to lakeFS Enterprise, follow the steps below:**

1. Sanity Test: Install fresh lakeFS enterprise: Test the setup > login > Create repository etc. Once everything seems to work delete and clean up, let's get to the real thing.
1. DB Migration: we are going to use the same DB for both lakeFS and Fluffy, so we need to migrate the DB schema.
1. Make sure to SSH / exec into the lakeFS server (old pre-upgrade version), the point is to use the same lakefs confugration file when running a migration.
   1. If upgrading `lakefs` version do this or skip to the next step: Install the new lakeFS binary, if not use the existing one (the one you are running).
   1. Run the command: `LAKEFS_AUTH_UI_CONFIG_RBAC=internal lakefs migrate up` (use the **new binary** if upgrading lakeFS version).
   1. You should expect to see a log message saying Migration completed successfully.
   1. During this short db migration process please make sure not to make any policy / RBAC related changes.
1. Once the migration completed - Upgrade your helm release to the new version and run `helm ugprade`, that’s it.

## Log Collection

The recommended practice for collecting logs would be sending them to the container std (default configuration)
and letting an external service to collect them to a sink. An example for logs collector would be [fluentbit](https://fluentbit.io/)
that can collect container logs, format them and ship them to a target like S3.

There are 2 kinds of logs, regular logs like an API error or some event description used for debugging
and audit_logs that are describing a user action (i.e create branch).
The distinction between regular logs and audit_logs is in the boolean field log_audit.
lakeFS and fluffy share the same configuration structure under logging.* section in the config.

{: .warning }
> The log entries marked with `log_audit = true` are currently available in both the open-source version of lakeFS and lakeFS Enterprise. However, please be aware that these log entries are deprecated in the open source version and will be removed in future releases.

[lakefs-sso-enterprise-spec]: {% link reference/security/sso.md %}#sso-for-lakefs-enterprise
[lakefs-sso-enterprise-spec-oidc]: {% link reference/security/sso.md %}#oidc
[lakefs-sso-enterprise-spec-saml]: {% link reference/security/sso.md %}#adfs
[lakefs-sso-enterprise-spec-ldap]: {% link reference/security/sso.md %}#ldap