---
title: Get Started
description: Methods for getting started with lakeFS Enterprise
parent: lakeFS Enterprise
has_children: true
has_toc: false
nav_order: 200
---

# Get Started with lakeFS Enterprise

## Install lakeFS Enterprise

lakeFS Enterprise offers multiple installation methods. Choose the one that best suits your use case. Consider the following when selecting an installation type:
* Your desired speed to get started
* Your familiarity with Kubernetes
* When you plan to test lakeFS Enterprise SSO support

If you can postpone the evaluation of the SSO integration, we suggest doing so to speed up overall testing. The SSO integration requires additional configurations and is best addressed later.

### Quickstart Install

Use the quickstart options to quickly evaluate lakeFS Enterprise. There are two quickstart methods:
* [Docker Quickstart]({% link enterprise/getstarted/quickstart.md#docker-quickstart %}) {: .d-inline-block }SSO supported{: .label .label-green }
* [Kubernetes Helm-based Quickstart]({% link enterprise/getstarted/quickstart.md#kubernetes-helm-chart-quickstart %})

**Note**
Quickstart installations are not suitable for production systems.
{: .note }

### Production Deployment
{: .d-inline-block }SSO supported{: .label .label-green }

For a production setup, use a Helm-based deployment on a Kubernetes cluster. This can be done on AWS, Azure, GCP, or on-premises.
