---
title: Reference
description: Reference documentation for the lakeFS platform's various APIs, CLIs, and file formats.
has_children: true
has_toc: false
nav_order: 20
---

# lakeFS Reference

<img src="/assets/img/docs_logo.png" alt="lakeFS Docs" width=200 style="float: right; margin: 0 0 10px 10px;"/>

## API

- [lakeFS API]({% link reference/api.md %})
- [S3 Gateway API]({% link reference/s3.md %})

## Components

- [Server Configuration]({% link reference/configuration.md %})
- lakeFS command-line tool [lakectl]({% link reference/cli.md %})

## Clients

- [Spark Metadata Client]({% link reference/spark-client.md %})
- [lakeFS Hadoop FileSystem]({% link integrations/spark.md%}#lakefs-hadoop-filesystem)
- [Python Client]({% link integrations/python.md %})

## Security

- [Authentication]({% link reference/security/authentication.md %})
- [Remote Authenticator]({% link reference/security/remote-authenticator.md %})
- [Role-Based Access Control (RBAC)]({% link reference/security/rbac.md %})
- [Presigned URL]({% link reference/security/presigned-url.md %})
- [Access Control Lists (ACLs)]({% link reference/security/access-control-lists.md %})
- [Single Sign On (SSO)]({% link reference/security/sso.md %})
- [Login to lakeFS with AWS IAM]({% link reference/security/external-principals-aws.md %})
  
## Other Reference Documentation

- [Monitoring using Prometheus]({% link reference/monitor.md %})
- [Auditing]({% link reference/auditing.md %})