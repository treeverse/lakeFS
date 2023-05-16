---
layout: default
title: Azure Private Link
description: Private Link enables lakeFS Cloud to interact with your infrastructure using private networking.
parent: lakeFS Cloud
has_children: false
---

# Azure Private Link for lakeFS

[Azure Private Link](https://learn.microsoft.com/en-us/azure/private-link/private-link-overview) enables secure access to Azure services from a private endpoint within your virtual network.
By using Azure Private Link with lakeFS, you can securely access lakeFS services without exposing traffic to the public internet.
In this manual, we will guide you through the steps to enable Azure Private Link to your lakeFS instance.

## Register your Azure subscription

To automatically approve private endpoint connections to the lakeFS network, please provide us with your subscription. If required, you can register multiple subscriptions.

## Create an Azure Private Link connection to lakeFS Cloud

Once your subscription is in our trusted subscriptions navigate to the Azure portal and do the following steps:
1. Navigate to the private endpoint
2. Click Create
3. On the first step (basics):
   - Select your subscription
   - Specify the desired resource group used to access lakeFS
   - Provide a name for your private endpoint instance
   - Specify the region of your lakeFS instance
4. On the second step (Resource)
   - In connection method select `connect to an Azure resource by resource ID or alias`
   - Insert the alias provided by us into the Resource ID or alias
   - No need to add a request message
5. Continue with the steps and run Review + Create

## Create a DNS entry for your private endpoint

Update your DNS server to resolve your account URL (which will be provided by us) to the Private Link IP address.
You can add the DNS entry to your on-premises DNS server or private DNS on your VNet, to access lakeFS services.
