---
title: Dremio
description: This section shows how you can start using lakeFS with Dremio, a next-generation data lake engine.
parent: Integrations
has_children: false
redirect_from: /using/dremio.html
---

# Using lakeFS with Dremio
[Dremio](https://www.dremio.com/) is a next-generation data lake engine that liberates your data with live, 
interactive queries directly on cloud data lake storage, including S3 and lakeFS.

## Configuration
Starting from version 3.2.3, Dremio supports Minio as an [experimental S3-compatible plugin](https://docs.dremio.com/current/sonar/data-sources/object/s3/#configuring-s3-for-minio).
Similarly, you can connect lakeFS with Dremio.

Suppose you already have both lakeFS and Dremio deployed, and want to use Dremio to query your data in the lakeFS repositories.
You can follow the steps listed below to configure on Dremio UI:

1. click _Add Data Lake_.
1. Under _File Stores_, choose _Amazon S3_.
1. Under _Advanced Options_, check _Enable compatibility mode (experimental)_.
1. Under _Advanced Options_ > _Connection Properties_, add `fs.s3a.path.style.access` and set the value to `true`.
1. Under _Advanced Options_ > _Connection Properties_, add `fs.s3a.endpoint` and set lakeFS S3 endpoint to the value. 
1. Under the _General_ tab, specify the _access_key_id_ and _secret_access_key_ provided by lakeFS server.
1. Click _Save_, and now you should be able to browse lakeFS repositories on Dremio.
