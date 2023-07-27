---
layout: default
title: Migrating away from lakeFS
description: The simplest way to migrate away from lakeFS is by copying data from a lakeFS repository to an S3 bucket.
parent: How-To
has_children: false
redirect_from: 
  - /deploying-aws/offboarding.html
  - /reference/offboarding.html
---

# Migrating away from lakeFS

## Copying data from a lakeFS repository to an S3 bucket

The simplest way to migrate away from lakeFS is by copying data from a lakeFS repository to an S3 bucket
(or any other object store).

For smaller repositories, you can do this by using the [AWS CLI]({{ site.baseurl }}/integrations/aws_cli.md) or [Rclone]({{ site.baseurl }}/howto/copying.md#using-rclone).
For larger repositories, running [distcp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html){: target="_blank"} with lakeFS as the source is also an option.

