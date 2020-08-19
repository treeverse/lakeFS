---
layout: default
title: Quick Start
nav_order: 2
has_children: false
---

# Quick Start
{: .no_toc }

**Note** This section is for learning purposes. The installations below will not persist you data.
For a production suitable deployment, see [Deploying on AWS](deploying/index.md).
{: .note }

## On Kubernetes with Helm
{: .no_toc }

You can install lakeFS on a Kubernetes cluster with the following commands:
```bash
# Add the lakeFS Helm repository
helm repo add lakefs https://charts.lakefs.io
# Deploy lakeFS with helm release "my-lakefs"
helm install my-lakefs lakefs/lakefs
```

This will start lakeFS with a dedicated PostgreSQL container. Data will be stored inside the container and will not be persisted.
You can now move on to [creating your first repository](#setting-up-a-repository). 

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Other Installation Options
###  Using Docker Compose

To run a local lakeFS instance, you can use the following example [Docker Compose](https://docs.docker.com/compose/){:target="_blank"} application:

1. Ensure you have Docker installed on your computer. The MacOS and Windows installations include [Docker Compose](https://docs.docker.com/compose/){:target="_blank"} by default.

1. Create a `docker-compose.yaml` file, containing the following configuration:

   ```yaml
   ---
   version: '3'
   services:
     lakefs:
       image: "treeverse/lakefs:latest"
       ports: ["8000:8000"]
       links: ["postgres"]
       environment:
         LAKEFS_AUTH_ENCRYPT_SECRET_KEY: some random secret string
         LAKEFS_DATABASE_CONNECTION_STRING: postgres://lakefs:lakefs@postgres/postgres?sslmode=disable
         LAKEFS_BLOCKSTORE_TYPE: local
         LAKEFS_BLOCKSTORE_LOCAL_PATH: /home/lakefs
         LAKEFS_GATEWAYS_S3_DOMAIN_NAME: s3.local.lakefs.io:8000
       entrypoint: ["/app/wait-for", "postgres:5432", "--", "/app/lakefs", "run"]
     postgres:
       image: "postgres:11"
       environment:
         POSTGRES_USER: lakefs
         POSTGRES_PASSWORD: lakefs
   ```

1. From the directory that contains our new `docker-compose.yaml` file, run the following command:

   ```bash
   $ docker-compose up
   ```

1. Open [http://localhost:8000/setup](http://localhost:8000/setup){:target="_blank"} in your web browser to set up an initial admin user, used to login and send API requests.


### Manual Installation 

Alternatively, you may opt to run the lakefs binary directly on your computer.

1. Download the lakeFS binary for your operating system:

   [Download lakefs](downloads.md){: .btn .btn-green target="_blank"}

1. Install and configure [PostgreSQL](https://www.postgresql.org/download/){:target="_blank"}

1. Create a configuration file:
    
   ```yaml
   ---
   database:
     connection_string: "postgres://localhost:5432/postgres?sslmode=disable"
    
   blockstore: 
     type: "local"
     local:
       path: "~/lakefs_data"
    
   auth:
     encrypt:
       secret_key: "a random string that should be kept secret"
   gateways:
     s3:
       domain_name: s3.local.lakefs.io:8000
   ```

1. Create a local directory to store objects:

   ```sh
   mkdir ~/lakefs_data
   ```

1. Run the server:
    
   ```bash
   $ ./lakefs --config /path/to/config.yaml run
   ```

## First Steps in lakeFS

### Setting up a Repository

1. Open [http://localhost:8000/setup](http://localhost:8000/setup){:target="_blank"} in your web browser to set up an initial admin user, used to login and send API requests.

   ![Setup](assets/img/setup.png)

1. Follow the steps to create an initial administrator user. Save the credentials you've received somewhere safe, you won't be able to see them again!

   ![Setup Done](assets/img/setup_done.png)

1. Follow the link and go to the login screen

   ![Login Screen](assets/img/login.png)

1. Use the credentials from step #2 to login as an administrator
1. Click `Create Repository`
    
   ![Create Repository](assets/img/repo_create.png)

   A [repository](branching/model.md#repositories) is lakeFS's basic namespace, akin S3's Bucket. (Read more about the data model [here](branching/model.md))
   Since we're using the `local` block adapter, the value used for `Storage Namespace` should be a static `local://`.
   For a real deployment that uses S3 as a block adapter, this would be a bucket name with optional prefix, i.e. `s3://my-bucket/prefix`. 
   {: .note .note-info }



### Copying files into lakeFS using AWS CLI

1. If you don't have the AWS CLI installed, follow the [instructions here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html){:target="_blank"}.
1. Configure a new connection profile using the credentials we generated earlier:

   ```bash
   $ aws configure --profile local
   AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
   AWS Secret Access Key [None]: ****************************************
   Default region name [None]:
   Default output format [None]:
   ```
1. Let's test to see that it works. We'll do that by calling `s3 ls` which should list our repositories for us:
   
   ```bash
   $ aws --endpoint-url=http://s3.local.lakefs.io:8000 --profile local s3 ls
     2020-05-18 17:47:03 example
   ```
   
   **Note:** We're using `s3.local.lakefs.io` - a special DNS record which always resolves to localhost, subdomains included.  
   Since S3's API uses [subdomains for bucket addressing](https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/){: target="_blank"}, simply using `localhost:8000` as an endpoint URL will not work.
   {: .note .note-info }

1. Great, now let's copy some files. We'll write to the master branch. This is done by prefixing our path with the name of the branch we'd like to read/write from:

   ```bash
   $ aws --endpoint-url=http://s3.local.lakefs.io:8000 --profile local s3 cp ./foo.txt s3://example/master/
   upload: ./foo.txt to s3://example/master/foo.txt
   ```

1. Back in the lakeFS UI, we should be able to see our file added to the master branch!

   ![Object Added](assets/img/object_added.png)

### CLI usage with lakectl

lakeFS comes with its own native CLI client. You can see the complete command reference [here](reference/commands.md).

The CLI is a great way to get started with lakeFS since it is a complete implementation of the lakeFS API.

Here's how to get started with the CLI:

1. Download the CLI binary:

   [Download lakectl](downloads.md){: .btn .btn-green target="_blank"}


1. It's recommended that you place it somewhere in your PATH (this is OS dependant but for *NIX systems , `/usr/local/bin` is usually a safe bet).
1. configure the CLI to use the credentials you've created earlier:

   ```bash
   $ lakectl config
   Config file /home/janedoe/.lakectl.yaml will be used
   Access key ID: AKIAIOSFODNN7EXAMPLE
   Secret access key: ****************************************
   Server endpoint URL: http://localhost:8000/api/v1
   ```

1. Now that we've configured it, let's run a few sample commands:

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

Once you're ready to test lakeFS with a real workflow, it's time to [deploy lakeFS to AWS](deploying/index.md).
