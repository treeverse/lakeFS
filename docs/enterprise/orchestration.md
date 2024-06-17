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
2. Fluffy - Proprietary: In charge of the Enterprise features. Can be retrieved from [Treeverse Dockerhub](https://hub.docker.com/u/treeverse) using the granted token, please [contact support](mailto:support@treeverse.io) to get access to the Dockerhub image.

## Quickstart with Docker Compose

### Prerequisites

1. Access to download [dockerhub/fluffy](https://hub.docker.com/u/treeverse) Docker image, to login locally `docker login -u <USERNAME> -p <TOKEN>`. Please [contact us](mailto:support@treeverse.io) to get access to the Dockerhub image.
2. [Docker Compose](https://docs.docker.com/compose/install/) installed version `2.23.1` or higher on your machine.

The following docker-compose file will spin up lakeFS, Fluffy and postgres as a shared KV database. 
For additional examples check out the [lakeFS Enterprise sample](https://github.com/treeverse/lakeFS-samples/tree/main/02_lakefs_enterprise) for all-in-one setup including storage and spark.

⚠️ Using a local postgres is not suitable for production use-cases.

<div class="tabs">
  <ul>
    <li><a href="#docker-compose-no-sso">NO SSO</a></li>
    <li><a href="#docker-compose-with-sso">With SSO (OIDC)</a></li>
  </ul> 
<div markdown="1" id="docker-compose-no-sso">

For simplicity the example does not use SSO and only supports basic authentication of access key and secret key.
    
1. Create `docker-compose.yaml` file with the following content
2. Run `docker compose up` in the same directory as the `docker-compose.yaml` file.
3. In your browser go to [http://localhost:8080](http://localhost:8080) to access lakeFS UI.

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

Next, create `docker-compose.yaml` file with the following content.

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

Test the OIDC configuration works - in your browser go to [http://localhost:8080](http://localhost:8080) to access lakeFS UI.

</div>
</div>


## Deploy lakeFS Enterprise (Kubernetes)


The following examples will guide you through the installation of lakeFS Enterprise using our [Helm Chart](#lakefs-helm-chart).

### Prerequisites

1. A KV Database, like postgres, should be configured and shared by fluffy and lakeFS.
1. Access to configure SSO IdP, like Azure AD Enterprise Application.
1. A proxy server should be configured to route traffic between the 2 servers.

### lakeFS Helm Chart Configuration

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
* Fluffy docker image: replace the `fluffy.image.privateRegistry.secretToken` with real token to dockerhub for the fluffy docker image.
* Check the [additional examples on GitHub](https://github.com/treeverse/charts/tree/master/examples/lakefs/enterprise) we provide for each authentication method (oidc, adfs, ldap, rbac, IAM etc).
* The Database configurations between fluffy and lakeFS should be the same since they connect to the same DB.

### Deploy lakeFS Chart (For Testing)

Same as the Docker Compose example, the following values file will deploy lakeFS with no SSO and local blockstore using dev postgres.

```yaml

```



# migration

[lakefs-migrate]: https://docs.lakefs.io/howto/deploy/upgrade.html