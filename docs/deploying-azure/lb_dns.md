---
layout: default
title: Load Balancing and DNS
description: Depending on how you chose to install lakeFS, you should have a load balancer direct requests to the lakeFS server.
parent: Azure Deployment
nav_order: 25
has_children: false
---

## Load balancing
Depending on how you chose to install lakeFS, you should have a load balancer direct requests to the lakeFS server.  
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.
For more information go to the [Azure Load Balancer documentation](https://docs.microsoft.com/en-us/azure/load-balancer/)
## DNS

You should create 3 DNS records for lakeFS:
1. One record for the lakeFS API: `lakefs.example.com`
1. Two records for the S3-compatible API: `s3.lakefs.example.com` and `*.s3.lakefs.example.com`.

All records should point to your Load Balancer.
For an Azure load balancer with Azure DNS zones, create a simple record, and choose *Alias to Application and Classic Load Balancer* with an `A` record type.

For more information go to the [Azure DNS documentation](https://docs.microsoft.com/en-us/azure/dns/)

For other DNS providers, refer to the documentation on how to add CNAME records.

In this case, it's recommended to use a short TTL value.

 
