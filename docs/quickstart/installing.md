---
layout: default
title: Install lakeFS
description: Installing lakeFS is easy. This section covers how to install lakeFS using docker compose.
parent: Quickstart
nav_order: 10
has_children: false
---

# Install lakeFS
{: .no_toc }

{% include learn_only.html %} 

## Using Docker Compose
{: .no_toc }

To run a local lakeFS instance using [Docker Compose](https://docs.docker.com/compose/){:target="_blank"}:

1. Ensure that you have Docker and Docker Compose installed on your computer, and that the Compose version is 1.25.04 or higher. For more information, please see this [issue](https://github.com/treeverse/lakeFS/issues/894). 

1. Run the following command in your terminal:

   ```bash
   curl https://compose.lakefs.io | docker-compose -f - up
   ```

1. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"} in your web browser.

1. [Create your first repository](repository.md) in lakeFS.

## Other methods

You can try lakeFS:

1. [On Kubernetes](more_quickstart_options.md#on-kubernetes-with-helm).
1. With docker-compose [on Windows](more_quickstart_options.md#docker-on-windows).
1. By [running the binary directly](more_quickstart_options.md#using-the-binary).

## Modifying the Compose file

To modify the local configuration file, for example, in order to use your local lakeFS environment against S3 storage as opposed to the local storage, download the configuration file https://compose.lakefs.io, modify it and then run the container with the modified copy:

```bash
docker-compose -f modified-docker-compose.yml up
```

For example, to run against S3 instead of local storage, change:
```bash
- LAKEFS_BLOCKSTORE_TYPE=${LAKEFS_BLOCKSTORE_TYPE:-local}

...

- LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-}

- LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_SECRET_KEY=${AWS_SECRET_ACCESS_KEY:-}
```

To:
```bash
- LAKEFS_BLOCKSTORE_TYPE=${LAKEFS_BLOCKSTORE_TYPE:-s3}

...

- LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID=###

- LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_SECRET_KEY=###
```

## Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).
