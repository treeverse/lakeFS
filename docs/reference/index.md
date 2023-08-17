---
title: Reference
description: Reference documentation for the lakeFS platform's various APIs, CLIs, and file formats.
has_children: true
has_toc: false
nav_order: 20
---

# lakeFS Reference

<img src="/assets/img/docs_logo.png" alt="lakeFS Docs" width=200 style="float: right; margin: 0 0 10px 10px;"/>

## Components

- [Server Configuration]({% link reference/configuration.md %})
- lakeFS command-line tool [lakectl]({% link reference/cli.md %})

## Clients

- [Spark Metadata Client]({% link reference/spark-client.md %})
- [lakeFS Hadoop FileSystem]({% link integrations/spark.md%}#lakefs-hadoop-filesystem)
- [Python Client](https://pydocs.lakefs.io/)

## Security

- [Authentication]({% link reference/authentication.md %})
- [Remote Authenticator]({% link reference/remote-authenticator.md %})
- [Role-Based Access Control (RBAC)]({% link reference/rbac.md %})
- [Presigned URL]({% link reference/presigned-url.md %})
- [Access Control Lists (ACLs)]({% link reference/access-control-lists.md %})

## Monitoring

- [Monitoring using Prometheus]({% link reference/monitor.md %})

## API

- [lakeFS API]({% link reference/api.md %})
- [S3 Gateway API]({% link reference/s3.md %})
