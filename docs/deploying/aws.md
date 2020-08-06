---
layout: default
title: Deploying on AWS
parent: Deployment
nav_order: 20
has_children: false
---

# Deploying on AWS
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Setting up DNS names for the OpenAPI Server and the S3 Gateway

1. Copy the load balancer's endpoint URL.
1. Configure this address in Route53 as an ALIAS record the load balancer endpoint.
1. If you're using a DNS provider other than Route53, refer to its documentation on how to add CNAME records. In this case, it's recommended to use a short TTL value.

## Automatically setup an environment using Terraform

*Terraform module coming soon*

## Setting up our environment

Once we have lakeFS configured and running, open `https://<OPENAPI_SERVER_ENDPOINT>/setup` (e.g. [https://lakefs.example.com](https://lakefs.example.com){: target="_blank" }).

1. Follow the steps to create an initial administrator user. Save the credentials you've received somewhere safe, you won't be able to see them again!

   ![Setup](../assets/img/setup_done.png)

2. Follow the link and go to the login screen

   ![Login Screen](../assets/img/login.png)

3. Use the credentials from step #1 to login as an administrator
4. Click `Create Repository`
    
   ![Create Repository](../assets/img/repo_create.png)

   Under `Storage Namespace`, be sure to use the name of the bucket you've created in [Setting up an S3 bucket for data storage](#setting-up-an-s3-bucket-for-data-storage) above.
   
## Next steps

Check out the usage guides under [using lakeFS with...](../using) to start using lakeFS with your existing systems!
