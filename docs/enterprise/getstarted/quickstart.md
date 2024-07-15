---
title: Quickstart
description: Quickstart guides for lakeFS Enterprise
parent: Get Started
grand_parent: lakeFS Enterprise
nav_order: 201
---

{% include toc.html %}

## Docker Quickstart

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


## Kubernetes Helm Chart Quickstart

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