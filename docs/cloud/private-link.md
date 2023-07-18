---
layout: default
title: Private Link
description: Private Link enables lakeFS Cloud to interact with your infrastructure using private networking.
parent: lakeFS Cloud
has_children: false
---

# Private Link
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

Private Link enables lakeFS Cloud to interact with your infrastructure using private networking.

{% include toc.html %}

## Supported Vendors

At the moment, we support Private-Link with AWS and Azure. If you are looking for Private Link for GCP please [contact us](mailto:support@treeverse.io).

<div class="tabs">
  <ul>
    <li><a href="#aws">AWS</a></li>
    <li><a href="#azure">Azure</a></li>
  </ul> 
  <div markdown="1" id="AWS">

## Access Methods

There are two types of Private Link implementation:

* **Front-End Access** refers to API and UI access. Use this option if you'd like your lakeFS application to be exposed only to your infrastructure and not to the whole internet.

* **Back-End Access** refers to the network communication between the lakeFS clusters we host, and your infrastructure. Use this option if you'd like lakeFS to communicate with your servers privately and not over the internet.

The two types of access are not mutually exclusive nor are they dependent on each other.

## Setting up Private Link

### Front-End Access

Prerequisites:
* Administrator access to your AWS account
* In order for us to communicate with your account privately, we'll need to create a service endpoint on our end first.

Steps:
1. Login to your AWS account
2. Go to AWS VPC Service
3. Filter the relevant VPC & Navigate to **Endpoints**
4. Click **Create endpoint**
5. Fill in the following:
    * **Name**: lakefs-cloud
    * **Service category**: Other endpoint services
    * **Service name**: input from Treeverse team (see prerequisites)
    * Click **Verify service**
    * Pick the VPC you'd like to expose this service to.
    * Click **Create endpoint**

Now you can access your infrastructure privately using the endpoint DNS name. If you would like to change the DNS name to a friendly one please contact [support@treeverse.io](mailto:support@treeverse.io).

### Back-End Access

Prerequisites:
* Administrator access to your AWS account

Steps:
1. Login to your AWS account
2. Go to AWS VPC Service
3. Filter the relevant VPC & Navigate to **Endpoints**
4. Click **endpoint service**
5. Fill in the following:
    * **Name**: lakefs-cloud
    * **Load Balancer Type**: Network
    * **Available load balancers**: pick the load balancer you'd like lakefs-cloud to send events to.
    * Click **Create**
6. Pick the newly created **Endpoint Service** from within the **Endpoint Services** page.
7. Navigate to the **Allow principals** tab.
8. Click **Allow principals**
9. Fill in the following ARN: `arn:aws:iam::924819537486:root`
10. Click **Allow principals**

That's it on your end! Now, we'll need the **service name** you've just created in order to associate it with our infrastructure, once we do, we'll be ready to use the back-end access privately.
 </div>

<div markdown="1" id="Azure">

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
 </div>
 </div>