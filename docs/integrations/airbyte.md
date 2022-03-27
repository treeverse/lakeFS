---
layout: default
title: Airbyte
description: Use Airbyte with lakeFS to easily sync data between applications and S3 with lakeFS version control. 
parent: Integrations
nav_order: 57
has_children: false
---

[Airbyte](https://airbyte.io//){: .button-clickable} is an open-source platform to sync data from applications, APIs & databases
to warehouses, lakes and other destinations. Using Airbyte's connectors you can get your data pipelines running
to consolidate many input sources.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }
   
## Using lakeFS with Airbyte
The integration between the two open-source projects brings resilience and manageability when using Airbyte
connectors to sync data to your S3 buckets by leveraging lakeFS branches and atomic commits and merges.

## Use-cases
You can leverage lakeFS ACID guarantees and [CI/CD capabilities](../usecases/ci.md){: .button-clickable} when ingesting data to S3 using lakeFS:

1. Consolidate many data source into a single branch and expose them to the consumers simultaneously when merging to the `main` branch.
1. Test the incoming data for breaking schema changes, using [lakeFS hooks](../setup/hooks.md){: .button-clickable}. 
1. Avoid having consumers reading partial data from connectors which failed half-way through the sync operation.
1. Experiment with ingested data before exposing it.

## S3 Connector
lakeFS exposes an [S3 Gateway](../understand/architecture.md#s3-gateway){: .button-clickable} that enables applications to communicate
with lakeFS the same way they would with Amazon S3.
You can use Airbyte's [S3 Destination](https://airbyte.io/destinations/s3){: .button-clickable} for uploading the data to lakeFS.

### Configuring lakeFS using the connector
Set the following parameters when creating a new Destination of type S3:

| Name             | Value                                                        | Example                                                                                                             |
|------------------|--------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| Endpoint         | The lakeFS S3 gateway URL                                    | `http://lakefs.example.com`                                                                                       |
| S3 Bucket Name   | The lakeFS repository where the data will be written         | `example-repo`                                                                                                      |
| S3 Bucket Path   | The branch and the path where the data will be written       | `main/data/from/airbyte` Where `main` is the branch name, and `data/from/airbyte` is the path under the branch. |
| S3 Bucket Region | Not applicable to lakeFS, use `us-east-1`                    | `us-east-1`                                                                                                         |
| S3 Key ID        | The lakeFS access key id used to authenticate to lakeFS.     | `AKIAlakefs12345EXAMPLE`                                                                                            |
| S3 Access Key    | The lakeFS secret access key used to authenticate to lakeFS. | `abc/lakefs/1234567bPxRfiCYEXAMPLEKEY`                                                                              |

**Note**
S3 Destination connector supports custom S3 endpoints strating from Airbyte's version  
[`v0.26.0-alpha`](https://github.com/airbytehq/airbyte/releases/tag/v0.26.0-alpha) released on Jun 17th 2021
{: .note}

The UI configuration will look like:

![S3 Destination Connector Configuration]({{ site.baseurl }}/assets/img/airbyte.png)
