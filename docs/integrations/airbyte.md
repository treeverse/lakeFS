---
layout: default
title: Airbyte
description: Use Airbyte with lakeFS to easily sync data between applications and S3 with lakeFS version control.
parent: Integrations
nav_order: 57
has_children: false
---

---
**Note:**
Currently, Airbyte and lakeFS integration is broken on the latest Airbyte version ([link to issue #11871](https://github.com/airbytehq/airbyte/issues/11871)).  
Until this issue is resolved, use the last known working version of Airbyte: [`v0.40.2`](https://github.com/airbytehq/airbyte/releases/tag/v0.40.2)
Please note that this also affects the use of lakeFS and Airbyte Cloud.
{: .note}
---

[Airbyte](https://airbyte.io//) is an open-source platform for syncing data from applications, APIs, and databases to
warehouses, lakes, and other destinations. You can use Airbyte's connectors to get your data pipelines to consolidate
many input sources.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }

## Using lakeFS with Airbyte
The integration between the two open-source projects brings resilience and manageability when you use Airbyte
connectors to sync data to your S3 buckets by leveraging lakeFS branches and atomic commits and merges.

## Use cases
You can take advantage of lakeFS consistency guarantees and [CI/CD capabilities](../using_lakefs/ci.md) when ingesting data to S3 using lakeFS:

1. Consolidate many data sources to a single branch and expose them to consumers simultaneously when merging to the `main` branch.
1. Test incoming data for breaking schema changes using [lakeFS hooks](../setup/hooks.md).
1. Prevent consumers from reading partial data from connectors which failed half-way through sync.
1. Experiment with ingested data on a branch before exposing it.

## S3 Connector
lakeFS exposes an [S3 Gateway](../understand/architecture.md#s3-gateway) that enables applications to communicate
with lakeFS the same way they would with Amazon S3.
You can use Airbyte's [S3 Destination](https://airbyte.io/destinations/s3) to upload data to lakeFS.

### Configuring lakeFS using the connector
Set the following parameters when creating a new Destination of type S3:

| Name             | Value                                                        | Example                                                                                                             |
|------------------|--------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| Endpoint         | The lakeFS S3 gateway URL                                    | `https://cute-axolotol.lakefs-demo.io`
| S3 Bucket Name   | The lakeFS repository where the data will be written         | `example-repo`                                                                                                      |
| S3 Bucket Path   | The branch and the path where the data will be written       | `main/data/from/airbyte` Where `main` is the branch name, and `data/from/airbyte` is the path under the branch. |
| S3 Bucket Region | Not applicable to lakeFS, use `us-east-1`                    | `us-east-1`                                                                                                         |
| S3 Key ID        | The lakeFS access key id used to authenticate to lakeFS.     | `AKIAlakefs12345EXAMPLE`                                                                                            |
| S3 Access Key    | The lakeFS secret access key used to authenticate to lakeFS. | `abc/lakefs/1234567bPxRfiCYEXAMPLEKEY`                                                                              |

**Note**
The S3 Destination connector supports custom S3 endpoints starting with Airbyte's version
[`v0.26.0-alpha`](https://github.com/airbytehq/airbyte/releases/tag/v0.26.0-alpha) released on Jun 17th 2021
{: .note}

The UI configuration will look as follows:

![S3 Destination Connector Configuration]({{ site.baseurl }}/assets/img/airbyte.png)
