---
layout: default
title: MinIO
description: This section covers how to use MinIO as the underlying storage for lakeFS.
parent: Integrations
nav_order: 47
has_children: false
redirect_from: ../using/minio.html
---

# Using lakeFS with MinIO

[MinIO](https://min.io){: .button-clickable} is a high performance, distributed object storage system. You can use lakeFS to add git-like capabilities over it.
For learning purposes, it is recommended to follow our [step-by-step guide](https://lakefs.io/git-like-operations-over-minio-with-lakefs/){: .button-clickable} on how to deploy lakeFS locally over MinIO.

If you already know how to install lakeFS, and want to configure it to use MinIO as the underlying storage, your lakeFS configuration should contain the following:

```yaml
blockstore:
  type: s3
  s3:
    force_path_style: true
    endpoint: http://<minio_endpoint>:9000
    discover_bucket_region: false
    credentials:
      access_key_id: <minio_access_key>
      secret_access_key: <minio_secret_key>
```

The full example can be found [here](https://docs.lakefs.io/reference/configuration.html#example-minio){: .button-clickable}.

Note that lakeFS can also be configured [using environment variables](https://docs.lakefs.io/reference/configuration.html#using-environment-variables){: .button-clickable}.


