---
layout: default
title: Quick Start
nav_order: 2
has_children: false
---

# Quick Start
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}


## Running a local server for testing

###  Using Docker Compose

**Note** This configuration stores data in-memory.
It is only intended for testing purposes
{: .note}

To run a local lakeFS instance, you can use the following example [Docker Compose](https://docs.docker.com/compose/){:target="_blank"} application:

1. Ensure you have Docker installed on your computer. The MacOS and Windows installations include [Docker Compose](https://docs.docker.com/compose/){:target="_blank"} by default.

2. Create a `docker-compose.yaml` file, containing the following configuration:

   ```yaml
   ---
   version: '3'
   services:
     lakefs:
       image: "treeverse/lakefs:latest"
       ports:
         - "8000:8000"
         - "8001:8001"
       links:
         - postgres
       environment:
         LAKEFS_AUTH_DB_URI: postgres://lakefs:lakefs@postgres/lakefsdb?search_path=lakefs_auth
         LAKEFS_AUTH_ENCRYPT_SECRET_KEY: some random secret string
         LAKEFS_METADATA_DB_URI: postgres://lakefs:lakefs@postgres/lakefsdb?search_path=lakefs_index
         LAKEFS_BLOCKSTORE_TYPE: mem
       entrypoint: ["/app/wait-for", "postgres:5432", "--", "/app/lakefs", "run"]
     postgres:
       image: "postgres:11"
       environment:
         POSTGRES_USER: lakefs
         POSTGRES_PASSWORD: lakefs
         POSTGRES_DB: lakefsdb
         LC_COLLATE: C
   ```

3. From the directory that contains our new `docker-compose.yaml` file, run the following command:

   ```bash
   $ docker-compose up
   ```

4. Open [http://localhost:8001/setup](http://localhost:8001/setup){:target="_blank"} in your web browser to set up an initial admin user, used to login and send API requests.


### Manual Installation 

Alternatively, you may opt to run the lakefs binary directly on your computer.

1. Download the lakeFS binary for your operating system:

   [Download lakefs](https://github.com){: .btn .btn-green target="_blank"}

2. Install and configure [PostgreSQL](https://www.postgresql.org/download/){:target="_blank"}

3. Create a PostgreSQL database:

   ```sh
   $ psql postgres
   ``` 

   ```sql
   CREATE DATABASE lakefsdb LC_COLLATE='C' TEMPLATE template0;
   ```

4. Create a configuration file:
    
   ```yaml
   ---
   metadata:
     db:
       uri: "postgres://localhost:5432/lakefsdb?search_path=lakefs_index"
    
   blockstore: 
     type: "local"
     local:
       path: "~/lakefs_data"
    
   auth:
     encrypt:
       secret_key: "a random string that should be kept secret"
     db:
       uri: "postgres://localhost:5432/lakefsdb?search_path=lakefs_auth"
   ```

5. Run the server:
    
   ```bash
   $ ./lakefs --config /path/to/config.yaml run
   ```

6. Open [http://localhost:8001/setup](http://localhost:8001/setup){:target="_blank"} in your web browser to set up an initial admin user, used to login and send API requests.

## Setting up a Repository

A [repository](what_is_lakefs.html#repositories) is lakeFS's basic namespace, akin S3's Bucket. (Read more about the data model [here](what_is_lakefs.html#branching-model))
Let's create one using the UI:

1. Open [http://localhost:8001/login](http://localhost:8001/login){:target="_blank"} in your web browser. Login using the credentials you've created during the installation.
2. Click `Create Repository`. Let's call our new repository `example`. Since we haven't configured an underlying S3 bucket, setting the storage namespace has no effect (when using an S3 block adapter, this value will be used to control the underlying bucket name to use). Let's set this to `example` as well.
    
   ![Create Repository](assets/img/create_repo.png)

3. We should now have a new repository called `example`. Time to load some data into it!

## Copying files into our local installation using the AWS CLI

1. If you don't have the AWS CLI installed, follow the [instructions here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html){:target="_blank"}.
2. Configure a new connection profile using the credentials we generated earlier:

   ```bash
   $ aws configure --profile local
   AWS Access Key ID [None]: AKIAJNYOQZSWBSSXURPQ
   AWS Secret Access Key [None]: ****************************************
   Default region name [None]:
   Default output format [None]:
   ```
3. Let's test to see that it works. We'll do that by calling `s3 ls` which should list our repositories for us:
   
   ```bash
   $ aws --endpoint-url=http://s3.local.lakefs.io:8000 --profile local s3 ls
     2020-05-18 17:47:03 example
   ```
   
   **Note:** We're using `s3.local.lakefs.io` - a special DNS record which always resolves to localhost, subdomains included.  
   Since S3's API uses [subdomains for bucket addressing](https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/){: target="_blank"}, simply using `localhost:8000` as an endpoint URL will not work.
   {: .note .note-info }

4. Great, now let's copy some files. We'll write to the master branch. This is done by prefixing our path with the name of the branch we'd like to read/write from:

   ```bash
   $ aws --endpoint-url=http://s3.local.lakefs.io:8000 --profile local s3 cp ./foo.txt s3://example/master/
   upload: ./foo.txt to s3://example/master/foo.txt
   ```

5. Back in the lakeFS UI, we should be able to see our file added to the master branch!

   ![Object Added](assets/img/object_added.png)

## CLI usage with lakectl

lakeFS comes with its own native CLI client. You can see the complete command reference [here](reference/commands.md).

The CLI is a great way to get started with lakeFS since it is a complete implementation of the lakeFS API.

Here's how to get started with the CLI:

1. Download the CLI binary:

   [Download lakectl](https://github.com){: .btn .btn-green target="_blank"}


2. It's recommended that you place it somewhere in your PATH (this is OS dependant but for *NIX systems , `/usr/local/bin` is usually a safe bet).
3. configure the CLI to use the credentials you've created earlier:

   ```bash
   $ lakectl config
   Config file /home/janedoe/.lakectl.yaml will be used
   Access key ID: AKIAJNYOQZSWBSSXURPQ
   Secret access key: ****************************************
   Server endpoint URL: http://localhost:8001/api/v1
   ```

4. Now that we've configured it, let's run a few sample commands:

   ```bash
   $ lakectl branch list lakefs://example
    +----------+------------------------------------------------------------------+
    | REF NAME | COMMIT ID                                                        |
    +----------+------------------------------------------------------------------+
    | master   | a91f56a7e11be1348fc405053e5234e4af7d6da01ed02f3d9a8ba7b1f71499c8 |
    +----------+------------------------------------------------------------------+
   
   $ lakectl commit lakefs://example@master -m 'added our first file!'
     Commit for branch "master" done.
     
     ID: 901f7b21e1508e761642b142aea0ccf28451675199655381f65101ea230ebb87
     Timestamp: 2020-05-18 19:26:37 +0300 IDT
     Parents: a91f56a7e11be1348fc405053e5234e4af7d6da01ed02f3d9a8ba7b1f71499c8

   $ lakectl log lakefs://example@master
     
     commit 901f7b21e1508e761642b142aea0ccf28451675199655381f65101ea230ebb87
     Author: Example User <user@example.com>
     Date: 2020-05-18 19:26:37 +0300 IDT
     
         added our first file!
     
   ```

## Next steps

Once you're ready to test lakeFS with a real workflow, it's time to [deploy lakeFS to AWS](deploying/aws.md).

