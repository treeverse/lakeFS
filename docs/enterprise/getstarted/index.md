---
title: Get Started
description: Methods for getting started with lakeFS Enterprise
parent: lakeFS Enterprise
has_children: true
has_toc: false
nav_order: 200
---

# Get Started with lakeFS Enterprise

## Get a Trial License

To start your 30-day free trial, please [contact us](https://lakefs.io/contact-sales/). You will be granted with a token that
allows downloading a Docker image that includes lakeFS Enterprise paid features.

## Install lakeFS Enterprise

lakeFS Enterprise offers multiple installation methods each designed for different use case. When selecting an installation type we recommend to consider the following:
* Your desired speed to get started
* Your familiarity with Kubernetes
* Is it a testing or production install
* When you plan to test lakeFS Enterprise SSO support: If you can postpone the evaluation of the SSO integration, we suggest doing so to speed up overall testing. The SSO integration requires additional configurations and is best addressed later.

### Quickstart

Use the [quickstart](quickstart.md) options to quickly evaluate lakeFS Enterprise. There are two quickstart methods:
* [Docker Quickstart](quickstart.md#docker-quickstart)
{: .d-inline-block }
SSO supported
{: .label .label-green }
* [Kubernetes Helm-based Quickstart](quickstart.md#kubernetes-helm-chart-quickstart)

**Note**
Quickstart installations are not suitable for production systems.
{: .note }

### Production Deployment
{: .d-inline-block }
SSO supported
{: .label .label-green }

For a production setup, use a Helm-based deployment on a Kubernetes cluster. This can be done on AWS, Azure, GCP, or on-premises.
