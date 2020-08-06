---
layout: default
title: Load Balancing and DNS
parent: Deployment
nav_order: 25
has_children: false
---

## Load balancing
Depending on how you chose to install lakeFS, you should have a load balancer direct requests to both the lakeFS server.  

## Using an AWS application load balancer:

1. Your security groups should allow the load balancer to access both the [S3 Gateway](../architecture.md#s3-gateway) and the [OpenAPI Server](../architecture.md#openapi-server)
1. Create a new load balancer using the AWS console.
1. Create a target group with a listener for port 8000
1. Setup TLS termination using the domain names you wish to use for both endpoints (i.e. `s3.lakefs.example.com`, `*.s3.lakefs.example.com`, `lakefs.example.com`).
1. Configure the health-check to use the exposed `/_health` URL


## Setting up DNS names for the OpenAPI Server and the S3 Gateway

1. Copy the load balancer's endpoint URL.
1. Configure this address in Route53 as an ALIAS record the load balancer endpoint.
1. If you're using a DNS provider other than Route53, refer to its documentation on how to add CNAME records.
In this case, it's recommended to use a short TTL value.
