---
title: Quickstart
description: Quickstart guides for lakeFS Enterprise
---

# Quickstart

Follow these quickstarts to try out lakeFS Enterprise.

!!! warning
lakeFS Enterprise Quickstarts are not suitable for production use-cases. See the [installation guide](install.md) to set up a production-grade lakeFS Enterprise installation

## lakeFS Enterprise Sample

The lakeFS Enterprise Sample is the quickest way to experience the value of lakeFS Enterprise features in a containerized environment. This Docker-based setup is ideal if you want
to easily interact with lakeFS without the hassle of integration and experiment with lakeFS without writing code.

By running the [lakeFS Enterprise Sample](https://github.com/treeverse/lakeFS-samples/tree/main/02_lakefs_enterprise), you will be getting a ready-to-use environment including
the following containers:

* lakeFS Enterprise (includes additional features)
* Postgres: used by lakeFS as a KV store
* MinIO container: used as the storage connected to lakeFS
* Jupyter notebooks setup: Pre-populated with [notebooks](https://github.com/treeverse/lakeFS-samples/blob/main/00_notebooks/00_index.ipynb) that demonstrate lakeFS Enterprise' capabilities
* Apache Spark: this is useful for interacting with data you'll manage with lakeFS

Checkout the [RBAC demo](https://github.com/treeverse/lakeFS-samples/blob/main/00_notebooks/rbac-demo.ipynb) notebook to see lakeFS Enterprise [Role-Based Access Control](../../security/access-control-lists.md) capabilities in action.

## Docker Quickstart

### Prerequisites

!!! note
To use lakeFS enterprise you must have:
- Access token to download binaries from Docker hub
- License to run lakeFS Enterprise
[Contact us](https://lakefs.io/contact-sales/) to gain access for both.


1. You have installed [Docker Compose](https://docs.docker.com/compose/install/) version `2.23.1` or higher on your machine.
2. Access to download *treeverse/lakefs-enterprise* from [Docker Hub](https://hub.docker.com/u/treeverse).
3. With the token you've been granted, login locally to Docker Hub with `docker login -u externallakefs -p <TOKEN>`.

<br>
The quickstart docker-compose files below create a lakeFS server that's connected to a [local blockstore](../../howto/deploy/onprem.md#local-blockstore) and spin up the following containers:

* lakeFS Enterprise
* Postgres: used by lakeFS as a KV store

You can choose from the following options:

1. Recommended: A fully functional lakeFS Enterprise setup without SSO support
2. Advanced: A fully functional lakeFS Enterprise setup including SSO support with OIDC integration configured

   !!! info
   If you can postpone the evaluation of the SSO integration, we suggest starting without it to speed up overall testing. The SSO integration requires additional configurations and is best addressed later.

=== "Recommended (SSO Disabled)"
1. Create a `docker-compose.yaml` file with the following content
2. Run `docker compose up` in the same directory as the `docker-compose.yaml` file.
3. In your browser, go to <http://localhost:8000> to access lakeFS UI.

    ```yaml
    version: "3"
    services:
      lakefs:
        image: "treeverse/lakefs-enterprise:latest"
        command: "RUN"
        ports:
          - "8000:8000"
        depends_on:
          - "postgres"
        environment:
          - LAKEFS_LISTEN_ADDRESS=0.0.0.0:8000
          - LAKEFS_LOGGING_LEVEL=DEBUG
          - LAKEFS_AUTH_ENCRYPT_SECRET_KEY=random_secret
          - LAKEFS_AUTH_UI_CONFIG_RBAC=internal
          - LAKEFS_DATABASE_TYPE=postgres
          - LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=postgres://lakefs:lakefs@postgres:5432/postgres?sslmode=disable
          - LAKEFS_BLOCKSTORE_TYPE=local
          - LAKEFS_BLOCKSTORE_LOCAL_PATH=/home/lakefs
          - LAKEFS_BLOCKSTORE_LOCAL_IMPORT_ENABLED=true
          - LAKEFS_AUTH_POST_LOGIN_REDIRECT_URL=http://localhost:8000/
          - LAKEFS_FEATURES_LOCAL_RBAC=true
          - LAKEFS_LICENSE_CONTENTS=<license token>
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

    configs:
      lakefs.yaml:
        content: |
          auth:
            ui_config:
              login_cookie_names:
                - internal_auth_session
    ```

=== "Advanced (SSO Enabled)"
This setup uses OIDC as the SSO authentication method thus requiring a valid OIDC configuration.

    1. Create a `docker-compose.yaml` with the content below.
    2. Create a `.env` file with the configurations below in the same directory as the `docker-compose.yaml`, docker compose will automatically use that.
    3. Run `docker compose up` in the same directory as the `docker-compose.yaml` file.
    4. Validate the OIDC configuration:
      * In your browser, go to <http://localhost:8000> to access lakeFS UI
      * Complete the Setup process, and login with your Admin credentials
      * Logout and try to login again, you will be redirected to the OIDC login page.

    `.env`

    ```
    LAKEFS_AUTH_PROVIDERS_OIDC_CLIENT_ID=<your-oidc-client-id>
    LAKEFS_AUTH_PROVIDERS_OIDC_CLIENT_SECRET=<your-oidc-client-secret>
    # The name of the query parameter that is used to pass the client ID to the logout endpoint of the SSO provider, i.e client_id
    LAKEFS_AUTH_PROVIDERS_OIDC_LOGOUT_CLIENT_ID_QUERY_PARAMETER=client_id
    LAKEFS_AUTH_PROVIDERS_OIDC_URL=https://my-sso.com/
    LAKEFS_AUTH_LOGOUT_REDIRECT_URL=https://my-sso.com/logout
    # Optional: display a friendly name in the lakeFS UI by specifying which claim from the provider to show (i.e name, nickname, email etc)
    LAKEFS_AUTH_OIDC_FRIENDLY_NAME_CLAIM_NAME=name
    LAKEFS_LICENSE_CONTENTS=<license token>
    ```

    `docker-compose.yaml`

    ```yaml
    version: "3"
    services:
      lakefs:
        image: "treeverse/lakefs-enterprise:latest"
        command: "RUN"
        ports:
          - "8000:8000"
        depends_on:
          - "postgres"
        environment:
          - LAKEFS_LISTEN_ADDRESS=0.0.0.0:8000
          - LAKEFS_LOGGING_LEVEL=DEBUG
          - LAKEFS_LOGGING_AUDIT_LOG_LEVEL=INFO
          - LAKEFS_AUTH_ENCRYPT_SECRET_KEY=shared-secret-key
          - LAKEFS_AUTH_LOGOUT_REDIRECT_URL=${LAKEFS_AUTH_LOGOUT_REDIRECT_URL}
          - LAKEFS_AUTH_UI_CONFIG_LOGIN_URL=/oidc/login
          - LAKEFS_AUTH_UI_CONFIG_LOGOUT_URL=/oidc/logout
          - LAKEFS_AUTH_UI_CONFIG_RBAC=internal
          - LAKEFS_AUTH_OIDC_FRIENDLY_NAME_CLAIM_NAME=${LAKEFS_AUTH_OIDC_FRIENDLY_NAME_CLAIM_NAME}
          - LAKEFS_AUTH_PROVIDERS_OIDC_ENABLED=true
          - LAKEFS_AUTH_PROVIDERS_OIDC_POST_LOGIN_REDIRECT_URL=http://localhost:8000/
          - LAKEFS_AUTH_PROVIDERS_OIDC_URL=${LAKEFS_AUTH_PROVIDERS_OIDC_URL}
          - LAKEFS_AUTH_PROVIDERS_OIDC_CLIENT_ID=${LAKEFS_AUTH_PROVIDERS_OIDC_CLIENT_ID}
          - LAKEFS_AUTH_PROVIDERS_OIDC_CLIENT_SECRET=${LAKEFS_AUTH_PROVIDERS_OIDC_CLIENT_SECRET}
          - LAKEFS_AUTH_PROVIDERS_OIDC_CALLBACK_BASE_URL=http://localhost:8000
          - LAKEFS_AUTH_PROVIDERS_OIDC_LOGOUT_CLIENT_ID_QUERY_PARAMETER=${LAKEFS_AUTH_PROVIDERS_OIDC_LOGOUT_CLIENT_ID_QUERY_PARAMETER}
          - LAKEFS_DATABASE_TYPE=postgres
          - LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=postgres://lakefs:lakefs@postgres:5432/postgres?sslmode=disable
          - LAKEFS_BLOCKSTORE_TYPE=local
          - LAKEFS_BLOCKSTORE_LOCAL_PATH=/tmp/lakefs/data
          - LAKEFS_BLOCKSTORE_LOCAL_IMPORT_ENABLED=true
          - LAKEFS_FEATURES_LOCAL_RBAC=true
          - LAKEFS_LICENSE_CONTENTS=${LAKEFS_LICENSE_CONTENTS}
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

    # This tweak is unfortunate but also necessary. logout_endpoint_query_parameters is a list
    # of strings which isn't parsed nicely as env vars.
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
            providers:
              oidc:
                logout_endpoint_query_parameters:
                  - returnTo
                  - http://localhost:8000/oidc/login
    ```

## Kubernetes Helm Chart Quickstart

In order to use lakeFS Enterprise, we provided out of the box setup, see [lakeFS Helm chart configuration](https://github.com/treeverse/charts/tree/master/charts/lakefs).

The values below create a fully functional lakeFS Enterprise setup without SSO support. The created setup is connected to a [local blockstore](../../howto/deploy/onprem.md#local-blockstore), and spins up the following pods:

* lakeFS Enterprise
* Postgres: used by lakeFS as a KV store


!!! info
If you can postpone the evaluation of the SSO integration, we suggest starting without it to speed up overall testing. The SSO integration requires additional configurations and is best addressed later. To
try lakeFS Enterprise SSO capability on a Kubernetes cluster, check out the [production deployment guide](install.md).

### Prerequisites

1. You have a Kubernetes cluster running in one of the platforms [supported by lakeFS](../../howto/deploy/index.md#deployment-and-setup-details).
2. [Helm](https://helm.sh/docs/intro/install/) is installed
3. Access to download *treeverse/lakefs-enterprise* from [Docker Hub](https://hub.docker.com/u/treeverse).
4. lakeFS Enterprise license
   [Contact us](https://lakefs.io/contact-sales/) to gain access to lakeFS Enterprise.

### Instructions

1. Add the lakeFS Helm repository with `helm repo add lakefs https://charts.lakefs.io`
1. Create a `values.yaml` file with the following content and make sure to replace `<lakefs-enterprise-docker-registry-token>` with the Docker Hub token you received, `<lakefs.acme.com>` and `<ingress-class-name>`.
1. In the desired K8S namespace run `helm install lakefs lakefs/lakefs -f values.yaml`
1. In your browser, go to the Ingress host to access lakeFS UI.

```yaml
enterprise:
  enabled: true

image:
  privateRegistry:
    enabled: true
    secretToken: <lakefs-enterprise-docker-registry-token>

lakefsConfig: |
  logging:
    level: "DEBUG"
  blockstore:
    type: local
  features:
    local_rbac: true
  auth:
    ui_config:
      rbac: internal

ingress:
  enabled: true
  ingressClassName: <ingress-class-name>
  annotations: {}
  hosts:
    - host: <lakefs.acme.com>
      paths:
       - /

# useDevPostgres is false by default and will override any other db configuration, 
# set false or remove for configuring your own db
useDevPostgres: true
```