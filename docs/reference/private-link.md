---
layout: default
title: Private-Link
description: This section covers Private-Link for lakeFS Cloud.
parent: Reference
nav_order: 65
has_children: false
---


# Private-Link
{: .d-inline-block }
Cloud
{: .label .label-green }


{: .note}
> Private-Link is only available for [lakeFS Cloud](../cloud.md).

{% include toc.html %}

## Supported Vendors

At the moment, we support Private-Link only on top of AWS.

Looking for Private-Link for Azure or GCP? [Contact us](mailto:support@treeverse.io)

## Front/Back End Access 

**Front-End Access** refers to the API/UI access, basically, if you'd like your lakeFS application to be exposed only to your infrastructure, and not to the whole internet, front-end access is what you're looking for.

**Back-End Access** refers to the network communication between the lakeFS clusters we host, and your infrastructure, if you'd like lakeFS to communicate with your servers privately, and not over the internet, back-end access is what you're looking for.

Front-End and Back-End access doesn't have to go together, and they don't conflict one with another, meaning, if you'd like only front-end access to be configured for private access your API, that's possible, same goes for the back-end access or both of them together.


## Onboarding

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

Now you can access your infrastructure privately using the endpoint DNS name, if you like, we can associate it with a friendly name, please contact support@treeverse.io for more information.

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

That's it on your end! Now, we'll need the **service name** you've just created in order to assosicate it with our infrastructure, once we do, we'll be ready to use the back-end access privately.