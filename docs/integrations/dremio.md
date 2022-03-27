---
layout: default
title: Dremio
description: This section covers how you can start using lakeFS with Dremio, a next-generation data lake engine.
parent: Integrations
nav_order: 90
has_children: false
redirect_from: ../using/dremio.html
---

# Using lakeFS with Dremio
[Dremio](https://www.dremio.com/){: .button-clickable} is a next-generation data lake engine that liberates your data with live, 
interactive queries directly on cloud data lake storage, including S3 and lakeFS.

## Configuration
Starting from 3.2.3, Dremio supports Minio as an [experimental S3-compatible plugin](https://docs.dremio.com/data-sources/s3.html#configuring-s3-for-minio){: .button-clickable}.
Similarly, we can connect lakeFS with Dremio.

Suppose you already have both lakeFS and Dremio deployed, and want to utilize Dremio to query your data in the lakeFS repositories.
You can follow below steps to configure on Dremio UI:

1. click `Add Data Lake`.
1. Under `File Stores`, choose `Amazon S3`.
1. Under `Advanced Options`, check `Enable compatibility mode (experimental)`.
1. Under `Advanced Options` > `Connection Properties`, add `fs.s3a.path.style.access` and set the value to true.
1. Under `Advanced Options` > `Connection Properties`, add `fs.s3a.endpoint` and set lakeFS S3 endpoint to the value. 
1. Under `Advanced Options` > `Connection Properties`, add `fs.s3a.path.style.access` and set to `true`
1. Under the `General` tab, specify the `access_key_id` and `secret_access_key` provided by lakeFS server.
1. Click `Save`, and now you should be able to browse lakeFS repositories on Dremio.
