---
layout: default
title: Install lakeFS
parent: How-To
description: This section will guide you through deploying and setting up a production lakeFS environment.
has_children: true
has_toc: false
nav_order: 1
redirect_from:
  - /setup/
  - /setup/storage/index.html
  - /setup/create-repo.html
  - /deploy/
---

# Deploy and Setup lakeFS

{: .tip }
> The instructions given here are for a self-managed deployment of lakeFS. 
> 
> For a hosted lakeFS service with guaranteed SLAs, try [lakeFS Cloud](https://lakefs.cloud)

## Deployment Overview

lakeFS has several dependencies for which you need to select and configure a technology or interface. 

* Object Store
* Authentication
* Key-Value Store
* Execution Platform

Which options are available depends on your deployment platform. For example, the object store available on Azure differs from that on AWS. 

![](/assets/img/deploy/deploy-lakefs.excalidraw.png)

## Deployment and Setup Details

* [AWS](aws.html)
* [Azure](azure.html)
* [GCP](gcp.html)
* [On-premises and other cloud providers](onprem.html)