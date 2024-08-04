---
title: Quickstart
description: Quickstart guides for lakeFS Enterprise
parent: Get Started
grand_parent: lakeFS Enterprise
nav_order: 201
---

# Quickstart

Follow these quickstarts to try out lakeFS Enterprise.

{: .note }
> ⚠️ lakeFS Enterprise Quickstarts are not suitable for production use-cases.
{% include toc.html %}

## lakeFS Enterprise Sample

The lakeFS Enterprise Sample is the quickest way to experience the value of lakeFS Enterprise features in a containerized environment. This Docker-based setup is ideal if you want
to easily interact with lakeFS without the hassle of integration and experiment with lakeFS without writing code.

By running the [lakeFS Enterprise Sample](https://github.com/treeverse/lakeFS-samples/tree/main/02_lakefs_enterprise), you will be getting a ready-to-use environment including
the following containers:
* lakeFS
* Fluffy (includes lakeFS Enterprise features)
* Postgres: used by lakeFS and Fluffy as a shared KV store
* MinIO container: used as the storage connected to lakFS
* Jupyter notebooks setup: Pre-populated with [notebooks](https://github.com/treeverse/lakeFS-samples/tree/main/00_notebooks) that demonstrate lakeFS Enterprise' capabilities
* Apache Spark: this is useful for interacting with data you'll manage with lakeFS

Checkout the [RBAC demo](https://github.com/treeverse/lakeFS-samples/blob/main/00_notebooks/rbac-demo.ipynb) notebook to see lakeFS Enterprise [Role-Based Access Control]({% link security/access-control-lists.md %}) capabilities in action.

## Docker Quickstart

### Prerequisites
{: .no_toc}

1. You have installed [Docker Compose](https://docs.docker.com/compose/install/) version `2.23.1` or higher on your machine.
2. Access to download *dockerhub/fluffy* from [Docker Hub](https://hub.docker.com/u/treeverse). [Contact us](https://lakefs.io/contact-sales/) to gain access to Fluffy.
3. With the token you've been granted, login locally to Docker Hub with `docker login -u externallakefs -p <TOKEN>`.

<br>
The quickstart docker-compose files below create a lakeFS server that's connected to a [local blockstore](../../howto/deploy/onprem.md#local-blockstore) and spin up the following containers:
* lakeFS
* Fluffy (includes lakeFS Enterprise features)
* Postgres: used by lakeFS and Fluffy as a shared KV store

You can choose from the the following options:
1. Recommended: A fully functional lakeFS Enterprise setup without SSO support
2. Advanced: A fully functional lakeFS Enterprise setup including SSO support with OIDC integration configured

{: .note }
> If you can postpone the evaluation of the SSO integration, we suggest starting without it to speed up overall testing. The SSO integration requires additional configurations and is best addressed later.

<br>
<div class="tabs">
  <ul>
    <li><a href="#docker-compose-no-sso">Recommended (SSO Disabled)
    </a></li>
    <li><a href="#docker-compose-with-sso">Advanced (SSO Enabled)</a></li>
  </ul>
<div markdown="1" id="docker-compose-no-sso">

1. Create a `docker-compose.yaml` file with the following content
2. Run `docker compose up` in the same directory as the `docker-compose.yaml` file.
3. In your browser, go to <http://localhost:8080> to access lakeFS UI.

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
1. Create a `docker-compose.yaml` with the content below.
2. Create a `.env` file with the configurations below in the same directory as the `docker-compose.yaml`, docker compose will automatically use that.
3. Run `docker compose up` in the same directory as the `docker-compose.yaml` file.
4. Validate the OIDC configuration:
  * In your browser, go to <http://localhost:8080> to access lakeFS UI
  * Complete the Setup process, and login with your Admin credentials
  * Logout and try to login again, you will be redirected to the OIDC login page.

`.env`

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

`docker-compose.yaml`

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


</div>
</div>

## Kubernetes Helm Chart Quickstart

In order to use lakeFS Enterprise and Fluffy, we provided out of the box setup, see [lakeFS Helm chart configuration](https://github.com/treeverse/charts/tree/master/charts/lakefs).

The values below create a fully functional lakeFS Enterprise setup without SSO support. The created setup is connected to a [local blockstore](../../howto/deploy/onprem.md#local-blockstore), and spins up the following pods:
* lakeFS
* Fluffy (includes lakeFS Enterprise features)
* Postgres: used by lakeFS and Fluffy as a shared KV store


{: .note }
> If you can postpone the evaluation of the SSO integration, we suggest starting without it to speed up overall testing. The SSO integration requires additional configurations and is best addressed later. To
> try lakeFS Enterprise SSO capability on a Kubernetes cluster, check out the [production deployment guide](install.md).

### Prerequisites
{: .no_toc}

1. You have a Kubernetes cluster running in one of the platforms [supported by lakeFS](../../howto/deploy/index.md#deployment-and-setup-details).
2. [Helm](https://helm.sh/docs/intro/install/) is installed
3. Access to download *dockerhub/fluffy* from [Docker Hub](https://hub.docker.com/u/treeverse). [Contact us](https://lakefs.io/contact-sales/) to gain access to Fluffy.

### Instructions
{: .no_toc}

1. Add the lakeFS Helm repository with `helm repo add lakefs https://charts.lakefs.io`
1. Create a `values.yaml` file with the following content and make sure to replace `<fluffy-docker-registry-token>` with the token Docker Hub token you recieved, `<lakefs.acme.com>` and `<ingress-class-name>`.
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
