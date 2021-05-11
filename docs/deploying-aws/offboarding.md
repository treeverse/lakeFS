---
layout: default
title: Migrating away from lakeFS
description: The simplest way to migrate away from lakeFS is to copy data from a lakeFS repository to an S3 bucket
parent: Production Deployment
nav_order: 40
has_children: false
---

# Migrating away from lakeFS

## Copying data from a lakeFS repository to an S3 bucket

The simplest way to migrate away from lakeFS is to copy data from a lakeFS repository to an S3 bucket
(or any other object store).

For smaller repositories, this could be done using the [AWS cli](../integrations/aws_cli.md) or [rclone](../integrations/rclone.md).
For larger repositories, running [distcp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html){: target="_blank"} with lakeFS as the source is also an option.

