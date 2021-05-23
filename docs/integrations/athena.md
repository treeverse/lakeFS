---
layout: default
title: Amazon Athena
description: This section covers how you can start querying data from lakeFS using Amazon Athena.
parent: Integrations
nav_order: 50
has_children: false
redirect_from: ../using/athena.html
---

# Using lakeFS with Amazon Athena
[Amazon Athena](https://aws.amazon.com/athena/) is an interactive query service that makes it easy to analyze data in Amazon S3 using standard SQL.
{:.pb-5 }


Amazon Athena works directly above S3 and can't access lakeFS.

In order to support querying data from lakeFS with Amazon Athena, we will use [create-symlink](glue_hive_metastore.md#create-symlink), one of the [metastore commands](glue_hive_metastore.md) in [lakectl](../reference/commands.md).

create-symlink receives a table in glue pointing to lakeFS and creates a copy of the table in glue pointing to the underlying S3 bucket.
We can then query the new created table with Athena


