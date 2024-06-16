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

1. Access to download [dockerhub/fluffy](https://hub.docker.com/r/treeverse/fluffy) Docker image, to login locally `docker login -u <USERNAME> -p <TOKEN>`. Please [contact us](mailto:support@treeverse.io) to get access to the Dockerhub image.
2. [Docker Compose](https://docs.docker.com/compose/install/) installed version `2.23.1` or higher on your machine.



The following docker-compose file will spin up lakeFS, Fluffy and postgres as a shared KV database. 

⚠️ Using a local postgres is not suitable for production use-cases.

<div class="tabs">
  <ul>
    <li><a href="#docker-compose-no-sso">NO SSO</a></li>
    <li><a href="#docker-compose-with-sso">With SSO</a></li>
  </ul> 
  <div markdown="1" id="docker-compose-no-sso">
    For simplicity, this example  does not use SSO and only supports basic authentication of access key and secret key.
    
    1. Create `docker-compose.yaml` file with the following content
    2. Run `docker compose up` in the same directory as the `docker-compose.yaml` file.

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
    This setup uses OIDC as the SSO authentication method.  
  </div>
</div>






