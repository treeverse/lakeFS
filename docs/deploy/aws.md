---
layout: default
title: On AWS
parent: Deploy lakeFS
description: 
nav_order: 10
---

# Deploy lakeFS on AWS
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

{% include_relative includes/prerequisites.md %}

## Creating the Database on AWS RDS
lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
We will show you how to create a database on AWS RDS, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

If you already have a database, take note of the connection string and skip to the [next step](#install-lakefs-on-ec2)

1. Follow the official [AWS documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_GettingStarted.CreatingConnecting.PostgreSQL.html){: target="_blank" } on how to create a PostgreSQL instance and connect to it.  
   You may use the default PostgreSQL engine, or [Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraPostgreSQL.html){: target="_blank" }. Make sure you're using PostgreSQL version >= 11.
2. Once your RDS is set up and the server is in `Available` state, take note of the endpoint and port.

   ![RDS Connection String](../assets/img/rds_conn.png)

3. Make sure your security group rules allow you to connect to the database instance.

## Install lakeFS on EC2
1. Save the following configuration file as `config.yaml`:

   ```yaml
   ---
   database:
     connection_string: "[DATABASE_CONNECTION_STRING]"
   auth:
     encrypt:
       # replace this with a randomly-generated string:
       secret_key: "[ENCRYPTION_SECRET_KEY]"
   blockstore:
     type: s3
     s3:
       region: us-east-1
   gateways:
     s3:
        # replace this with the host you will use for the lakeFS S3-compatible endpoint:
        domain_name: [S3_GATEWAY_DOMAIN]
   ```

1. [Download the binary](../index.md#downloads) to the EC2 instance.
1. Run the `lakefs` binary on the EC2 instance:
   ```bash
   lakefs --config config.yaml run
   ```
   **Note:** it is preferable to run the binary as a service using systemd or your operating system's facilities.

## Load balancing
Depending on how you chose to install lakeFS, you should have a load balancer direct requests to the lakeFS server.  
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.

### Notes for using an AWS Application Load Balancer
{: .no_toc }

1. Your security groups should allow the load balancer to access the lakeFS server.
1. Create a target group with a listener for port 8000.
1. Setup TLS termination using the domain names you wish to use for both endpoints (e.g. `s3.lakefs.example.com`, `*.s3.lakefs.example.com`, `lakefs.example.com`).
1. Configure the health-check to use the exposed `/_health` URL

## DNS on AWS Route53
As mentioned above, you should create 3 DNS records for lakeFS:
1. One record for the lakeFS API: `lakefs.example.com`
1. Two records for the S3-compatible API: `s3.lakefs.example.com` and `*.s3.lakefs.example.com`.

For an AWS load balancer with Route53 DNS, create a simple record, and choose *Alias to Application and Classic Load Balancer* with an `A` record type.

![Configuring a simple record in Route53](../assets/img/route53.png)

For other DNS providers, refer to the documentation on how to add CNAME records.

You can now move on to the [Setup](../guides/setup.md) page.

## Next Steps
You can now move on to the [Setup](../guides/setup.md) page.

{% include_relative includes/why-dns.md %}
