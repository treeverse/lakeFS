---
layout: default
title: Quick Start
nav_order: 2
has_children: false
---

# Quick Start


## Running a local server for testing

###  Using Docker Compose

**Note** This configuration stores data in-memory.
It is only intended for testing purposes
{: .note}

To run a local lakeFS instance, you can use the following example [Docker Compose](https://docs.docker.com/compose/) application:

```yaml
---
version: '3'
services:
  lakefs:
    image: "lakefs:latest"
    ports:
      - "8000:8000"
      - "8001:8001"
    volumes:
      - "./lakefs_config.yaml:/etc/lakefs.yaml"
    links:
      - postgres
    command: []
    entrypoint: [
      "/app/wait-for", "postgres:5432", "--", 
      "/app/lakefs", "--config", "/etc/lakefs.yaml", "run"
    ]
  postgres:
    image: "postgres:11"
    environment:
      POSTGRES_USER: lakefs
      POSTGRES_PASSWORD: lakefs
      POSTGRES_DB: lakefs
      LC_COLLATE: C
```

With a corresponding configuration file (should be in the same directory as the `docker-compose.yaml` file), name `lakefs_config.yaml`:

```yaml
---
blockstore: 
  type: "mem"

metadata:
  db:
    uri: "postgres://lakefs:lakefs@postgres/lakefs?search_path=lakefs_index"

auth:
  encrypt:
    secret_key: "a random string that should be kept secret"
  db:
    uri: "postgres://lakefs:lakefs@postgres/lakefs?search_path=lakefs_auth"
``` 

Once we have this configuration in place, we can run the application:

```bash
$ docker-compose up
```

And open [http://localhost:8001/setup](http://localhost:8001/setup) in your web browser to set up an initial admin user, used to login and send API requests.


### Manual Installation 

Alternatively, you may opt to run the lakefs binary directly on your computer.

1. [Download](https://github.com) the lakeFS binary suitable for your platform

2. Install and configure [PostgreSQL]()

3. Create a PostgreSQL database:

    ```sql
    CREATE DATABASE lakefsdb LC_COLLATE='C' TEMPLATE template0;
    ``` 

4. Create a configuration file:
    
    ```yaml
    ---
    blockstore: 
      type: "local"
      local:
        path: "~/lakefs_data"
    
    metadata:
      db:
        uri: "postgres://localhost:5432/lakefsdb?search_path=lakefs_index"
    
    auth:
      encrypt:
        secret_key: "a random string that should be kept secret"
      db:
        uri: "postgres://localhost:5432/lakefsdb?search_path=lakefs_auth"
    ```

5. Run the server:
    
    ```sh
    $ ./lakefs --config /path/to/config.yaml run
    ```

6. Open [http://localhost:8001/setup](http://localhost:8001/setup) in your web browser to set up an initial admin user, used to login and send API requests.

## Setting up our first repository

A [repository]() is lakeFS's basic namespace, akin S3's Bucket.
Let's create one using the UI:

1. Open [http://localhost:8001/login](http://localhost:8001/login) in your web browser. Login using the credentials you've created during the installation.
2. Click `Create Repository`. Let's call our new repository `example`. Since we haven't configured an underlying S3 bucket, setting the storage namespace has no effect (when using an S3 block adapter, this value will be used to control the underlying bucket name to use). Let's set this to `example` as well.
    
    ![Create Repository](assets/img/create_repo.png)

3. We should now have a new repository called `example`. Time to load some data into it!

## Using the AWS CLI to copy files into our local installation

1. If you don't have the AWS CLI installed, follow the [instructions here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
2. Configure a new connection profile using the credentials we generated earlier:

    ```sh
    $ aws configure --profile local
    AWS Access Key ID [None]: AKIAJNYOQZSWBSSXURPQ
    AWS Secret Access Key [None]: ****************************************
    Default region name [None]:
    Default output format [None]:
    ```
3. Let's test to see that it works. We'll do that by calling `s3 ls` which should list our repositories for us:

    ```sh
    $ aws --endpoint-url=http://s3.local.lakefs.io:8000 --profile local s3 ls
      2020-05-18 17:47:03 example
    ```

4. Great, now let's copy some files. We'll write to the master branch. This is done by prefixing our path with the name of the branch we'd like to read/write from:

    ```sh
    $ aws --endpoint-url=http://s3.local.lakefs.io:8000 --profile local s3 cp ./foo.txt s3://example/master/
    upload: ./foo.txt to s3://example/master/foo.txt
    ```

## CLI usage with lakectl

## lakeFS UI
