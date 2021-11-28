---
layout: default
title: Azure Blob Storage
description: This guide explains how to configure Azure Blob Storage as the underlying storage layer.
parent: Prepare Your Storage
grand_parent: Setup lakeFS
nav_order: 30
has_children: false
---

# Prepare Your Blob Storage Container

[Create a container in Azure portal](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container):

1. From the Azure portal, Storage Accounts, choose your account, then in the container tab click `+ Container`.
1. Make sure you block public access

## Authenticate with Secret Key
{: .no_toc }

If you want lakeFS to authenticate with your storage using the storage account key, go to the `Access Keys` tab and click `Show Keys`. Use the values under `Storage account name` and `Key` in the [lakeFS configuration](../../deploy/azure.html#on-azure-vm).

## Authenticate with Active Directory
{: .no_toc }

In case you want your lakeFS Installation (we will install in the next step) to access this Container using Active Directory authentication,
First go to the container you created in step 1.
* Go to `Access Control (IAM)`
* Go to the `Role assignments` tab
* Add the `Storage Blob Data Contributor` role to the Installation running lakeFS.

You are now ready to [create your first lakeFS repository](../create-repo.md).
