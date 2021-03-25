---
layout: default
title: Configuring the Azure container
description: Information on configuring the Azure container to provide the data storage layer for our installation. 
parent: Azure Deployment
nav_order: 15
has_children: false
---

# Configuring the Azure container

The Azure container will provide the data storage layer for our installation.
You can choose to create a new container (recommended) or use an existing container with a path prefix.
The path under the existing container should be empty.

[create a container in Azure portal](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container)
1. From the Azure portal, Storage Accounts, choose your account, then in the container tab click `+ Container`.
2. Make sure you block public access

## Authenticate with Secret Key
In case you want to use the secret key for authentication you will need to use the account key in the configuration
Go to the `Access Keys` tab and click on `Show Keys` save the values under `Storage account name` and `Key` we will need them in the [installing lakeFS](install.md) step 
## Authenticate with Active Directory
In case you want your lakeFS Installation (we will install in the next step) to access this Container using Active Directory authentication,
First go to the container you created in step 1.
* Go to `Access Control (IAM)` 
* Go to the `Role assignments` tab 
* Add the `Storage Blob Data Contributor` role to the Installation running lakeFS.

