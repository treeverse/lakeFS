---
title: Run lakeFS Enterprise
description: Start using lakeFS-enterprise
parent: lakeFS Enterprise
---

# Run lakeFS Enterprise

{% include toc.html %}

## Overview

lakeFS Enterprise solution consists of 2 main components:
1. lakeFS - Open Source: [treeverse/lakeFS](https://hub.docker.com/r/treeverse/lakefs),
   release info found in [Github releases](https://github.com/treeverse/lakeFS/releases).
2. Fluffy - Proprietary: In charge of the Enterprise features. Can be retrieved from
   [Treeverse Dockerhub](https://hub.docker.com/u/treeverse) using the granted token.

### Prerequisites

1. A KV Database, like postgres, should be configured and shared by fluffy and lakeFS.
1. Access to configure SSO IdP, like Azure AD Enterprise Application.
1. A proxy server should be configured to route traffic between the 2 servers.


There are several ways to run lakeFS Enterprise. You can follow the examples below,
or create your own setup using our resources available in our documentation.

## Using lakeFS Helm chart

With every new release of lakeFS or Fluffy, the lakeFS team releases a new [lakeFS 
helm chart](https://artifacthub.io/packages/helm/lakefs/lakefs). Together with the granted
Fluffy token, you can run the full lakeFS Enterprise solution.

As an example, the following `values` file will run lakeFS Enterprise with OIDC integration.

```yaml
lakefsConfig: | 
  logging:
      level: "INFO"
  blockstore:
    type: s3
  database:
    type: postgres
    postgres:
      connection_string: <postgres-connection-string>
  auth:
    oidc:
      # the claim that's provided by the OIDC provider (e.g Okta) that will be used as the username according to OIDC provider claims provided after successful authentication
      friendly_name_claim_name: "<some-oidc-provider-claim-name>"
      default_initial_groups: ["Developers"]
    ui_config:
      login_cookie_names:
        - internal_auth_session
        - oidc_auth_session
ingress:
  enabled: true
  ingressClassName: <class-name>
  hosts:
    # the ingress that will be created for lakeFS
    - host: <lakefs.ingress.domain>
      paths: 
       - /

##################################################
########### lakeFS enterprise - FLUFFY ###########
##################################################

fluffy:
  enabled: true
  image:
    repository: treeverse/fluffy
    tag: '0.4.0'
    pullPolicy: IfNotPresent
    privateRegistry:
      enabled: true
      secretToken: <dockerhub-token-fluffy-image>
  fluffyConfig: |
    logging:
      format: "json"
      level: "INFO"
    database:
      type: postgres
      postgres:
        connection_string: <postgres-connection-string>
    auth:
      serve_listen_address: 0.0.0.0:9000
      logout_redirect_url: https://oidc-provider-url.com/logout/example
      oidc:
        enabled: true
        url: https://oidc-provider-url.com/
        client_id: <oidc-client-id>
        callback_base_url: https://<lakefs.ingress.domain>
        is_default_login: true
        # the claim name that represents the client identifier in the OIDC provider (e.g Okta)
        logout_client_id_query_parameter: client_id
        # the query parameters that will be used to redirect the user to the OIDC provider (e.g Okta) after logout
        logout_endpoint_query_parameters:
          - returnTo
          - https://<lakefs.ingress.domain>/oidc/login
  secrets:
    create: true
  sso:
    enabled: true
    oidc:
      enabled: true
      # secret given by the OIDC provider (e.g auth0, Okta, etc)
      client_secret: <oidc-client-secret>
  rbac:
    enabled: true

useDevPostgres: true
```

1. The example uses OIDC authentication method. For other methods, see [SSO]({% link reference/security/sso.md %}).
2. Database configuration must be identical between lakeFS and Fluffy.
3. `useDevPostgres` isn't suitable for production. When used, a local dev postgres is created.


## Using docker compose

The following docker-compose file will spin up lakeFS, Fluffy and postgres as a shared KV database. 
This setup uses OIDC as the SSO authentication method.
Using a local postgres is not suitable for production use-cases.

```yaml
version: "3"
services:
  lakefs:
    image: "treeverse/lakefs:1.20.0"
    command: "RUN"
    ports:
      - "8080:8000"
    depends_on:
      - "postgres"
    environment:
      - LAKEFS_LOGGING_LEVEL=DEBUG
      - LAKEFS_AUTH_ENCRYPT_SECRET_KEY="some random secret string"
      - LAKEFS_AUTH_API_ENDPOINT=http://fluffy:9000/api/v1
      - LAKEFS_AUTH_API_SUPPORTS_INVITES=true
      - LAKEFS_AUTH_LOGOUT_REDIRECT_URL=http://fluffy:8000/oidc/logout
      - LAKEFS_AUTH_UI_CONFIG_LOGIN_URL=http://fluffy:8000/oidc/login
      - LAKEFS_AUTH_UI_CONFIG_LOGOUT_URL=http://fluffy:8000/oidc/logout
      - LAKEFS_AUTH_UI_CONFIG_RBAC=internal
      - LAKEFS_AUTH_UI_CONFIG_LOGIN_COOKIE_NAMES=[internal_auth_session,oidc_auth_session]
      - LAKEFS_AUTH_OIDC_FRIENDLY_NAME_CLAIM_NAME="nickname"
      - LAKEFS_AUTH_OIDC_DEFAULT_INITIAL_GROUPS=["Admins"]
      - LAKEFS_AUTH_AUTHENTICATION_API_ENDPOINT=http://fluffy:8000/api/v1
      - LAKEFS_AUTH_AUTHENTICATION_API_EXTERNAL_PRINCIPALS_ENABLED=true
      - LAKEFS_DATABASE_TYPE=postgres
      - LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=postgres://lakefs:lakefs@postgres/postgres?sslmode=disable
      - LAKEFS_BLOCKSTORE_TYPE=local
      - LAKEFS_BLOCKSTORE_LOCAL_PATH=/home/lakefs
      - LAKEFS_BLOCKSTORE_LOCAL_IMPORT_ENABLED=true
    entrypoint: ["/app/wait-for", "postgres:5432", "--", "/app/lakefs", "run"]

  postgres:
    image: "postgres:11"
    ports:
      - "5433:5432"
    environment:
      POSTGRES_USER: lakefs
      POSTGRES_PASSWORD: lakefs

  fluffy:
    image: "${FLUFFY_REPO:-treeverse}/fluffy:${TAG:-0.4.0}"
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
      - FLUFFY_AUTH_ENCRYPT_SECRET_KEY="some random secret string"
      - FLUFFY_AUTH_SERVE_LISTEN_ADDRESS=0.0.0.0:9000
      - FLUFFY_LISTEN_ADDRESS=0.0.0.0:8000
      - FLUFFY_AUTH_SERVE_DISABLE_AUTHENTICATION=true
      - FLUFFY_AUTH_LOGOUT_REDIRECT_URL=<oidc-login-url>
      - FLUFFY_AUTH_POST_LOGIN_REDIRECT_URL=http://lakefs:8000/
      - FLUFFY_AUTH_OIDC_ENABLED=true
      - FLUFFY_AUTH_OIDC_URL=<oidc-endpoint>
      - FLUFFY_AUTH_OIDC_CLIENT_ID=<client-id>
      - FLUFFY_AUTH_OIDC_CLIENT_SECRET=<client-secret>
      - FLUFFY_AUTH_OIDC_CALLBACK_BASE_URL=http://fluffy:8000
      - FLUFFY_AUTH_OIDC_IS_DEFAULT_LOGIN=true
      - FLUFFY_AUTH_OIDC_LOGOUT_CLIENT_ID_QUERY_PARAMETER=client_id
    entrypoint: [ "/app/wait-for", "postgres:5432", "--", "/app/fluffy" ]
    configs:
      - source: fluffy.yaml
        target: /etc/fluffy/config.yaml

 #This tweak is unfortunate but also necessary. logout_endpoint_query_parameters is a list
 #of strings which isn't parsed nicely as env vars.
configs:
  fluffy.yaml:
    content: |
      auth:
        oidc:
          logout_endpoint_query_parameters:
            - returnTo
            - http://localhost:8080/oidc/login
```

## More examples

Few more examples for running lakeFS Enterprise based on the SSO provider and the setup. 

### Active Directory Federation Services (AD FS) (using SAML) without helm
{:.no_toc}

**Note:** If you'd like to run this example on a k8s cluster, follow this section
and replace fluffy and lakeFS configuration in the helm's `values.yaml` file.
{: .note }

#### Azure Configuration

1. Create an Enterprise Application with SAML toolkit - see [Azure quickstart](https://learn.microsoft.com/en-us/entra/identity/enterprise-apps/add-application-portal)
1. Add users: **App > Users and groups**: Attach users and roles from their existing AD users
   list - only attached users will be able to login to lakeFS.
1. Configure SAML: App >  Single sign-on > SAML:
   1. Entity ID: Add 2 ID’s, lakefs-url + lakefs-url/saml/metadata (e.g. https://lakefs.acme.com)
   1. Reply URL: lakefs-url/saml (e.g. https://lakefs.acme.com/saml)
   1. Sign on URL: lakefs-url/sso/login-saml (e.g. https://lakefs.acme.com/sso/login-saml)
   1. Relay State (Optional): /

#### Fluffy Configuration

**Note:** Full Fluffy configuration can be found [here]({% link enterprise/fluffy-configuration.md %})..
{: .note }

1. `auth.saml.idp_metadata_url`: Set from the Azure app created above _SAML configuration > “App Federation Metadata Url”_
1. `auth.saml.external_user_id_claim_name`: The claim that represents the UserID.
   The saim claim name must also be set in lakeFS config key `auth.cookie_auth_verification.external_user_id_claim_name`.
   The example below uses http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name as the claim name.

```yaml
# fluffy configuration.yaml 

# SSO address (i.e login with Azure) and healthcheck address /_health
listen_address: :8000
logging:
  level: "INFO"
  audit_log_level: "INFO"
# everything under database equal to the config in lakeFS
database:
  type: postgres
  postgres:
    # same as lakeFS
    connection_string: <postgres connection string> 
auth:
  # RBAC Service, default address is 0.0.0.0:9000, also lakeFS healcheck
  serve_listen_address: ':9000'
  encrypt:
    # same as lakeFS
    secret_key: shared-secrey-key 
  logout_redirect_url: https://<lakefs-url>
  post_login_redirect_url: https://<lakefs-url>
  saml:
    enabled: true
    sp_root_url: https://<lakefs-url>
    # generated SSL key, if not enforced on on the IdP level then can be anything as long as it a valid cert structure. 
    sp_x509_key_path: '/etc/saml_certs/rsa_saml_private.cert'
    sp_x509_cert_path: '/etc/saml_certs/rsa_saml_public.pem'
    sp_sign_request: false
    sp_signature_method: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
    idp_metadata_url: https://login.microsoftonline.com/<...>/federationmetadata/2007-06/federationmetadata.xml?appid=<app-id>
    external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name
    idp_skip_verify_tls_cert: true
```

#### lakeFS Configuration

```yaml
# lakeFS configuration.yaml 

database:
  type: postgres
  postgres:
    # same as fluffy
    connection_string: <postgres connection string> 
auth:
  encrypt:
    # same as fluffy
    secret_key: shared-secrey-key 
  api:
    # RBAC endpoint
    endpoint: http://<fluffy>:9000/api/v1
  cookie_auth_verification:
    auth_source: saml
    friendly_name_claim_name: displayName
    external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name
    default_initial_groups:
      - "Developers"
  ui_config:
    login_url: "https://<lakefs-url>/sso/login-saml"
    logout_url: "https://<lakefs-url>/sso/logout-saml"
    rbac: internal
    login_cookie_names:
       - internal_auth_session
       - saml_auth_session
```

### LDAP `values.yaml` file for helm deployments
{:.no_toc}

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
    - host: <lakefs.ingress.domain>
      paths: 
       - /

##################################################
########### lakeFS enterprise - FLUFFY ###########
##################################################

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

## Log Collection

The recommended practice for collecting logs would be sending them to the container std (default configuration)
and letting an external service to collect them to a sink. An example for logs collector would be [fluentbit](https://fluentbit.io/)
that can collect container logs, format them and ship them to a target like S3.

There are 2 kinds of logs, regular logs like an API error or some event description used for debugging
and audit_logs that are describing a user action (i.e create branch).
The distinction between regular logs and audit_logs is in the boolean field log_audit.
lakeFS and fluffy share the same configuration structure under logging.* section in the config.