---
layout: default
title: Migrating away from lakeFS
description: The simplest way to migrate away from lakeFS is to copy data from a lakeFS repository to an S3 bucket
parent: Reference
nav_order: 40
has_children: false
redirect_from: ../deploying-aws/offboarding.html
---

# Migrating away from lakeFS

## Copying data from a lakeFS repository to an S3 bucket

The simplest way to migrate away from lakeFS is to copy data from a lakeFS repository to an S3 bucket
(or any other object store).

For smaller repositories, this could be done using the [AWS cli](../integrations/aws_cli.md){: .button-clickable} or [rclone](../integrations/rclone.md){: .button-clickable}.
For larger repositories, running [distcp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html){: target="_blank" .button-clickable} with lakeFS as the source is also an option.

