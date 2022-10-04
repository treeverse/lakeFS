---
layout: default
title: Install lakeFS
description: Installing lakeFS is easy. This section covers how to spin up lakeFS using Docker.
parent: Quickstart
nav_order: 10
has_children: false
---

# Install lakeFS
{: .no_toc }

{% include learn_only.html %} 

## Using Docker 
{: .no_toc }

To run a local lakeFS instance using [Docker](https://docs.docker.com/){:target="_blank"}:

1. Ensure that you have Docker installed on your computer. 

1. Run the following command in your terminal:

   ```bash
   docker run --pull always -p 8000:8000 treeverse/lakefs run --local-settings
   ```

1. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"} in your web browser.

1. [Create your first repository](repository.md) in lakeFS.

## Other methods

You can try lakeFS:

1. [On Kubernetes](more_quickstart_options.md#on-kubernetes-with-helm).
1. By [running the binary directly](more_quickstart_options.md#using-the-binary).

## Modifying the local deployment

To modify the local deployment, for example, in order to use your local lakeFS against S3 storage (as opposed to the local storage), run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 -e LAKEFS_BLOCKSTORE_TYPE='s3' -e AWS_ACCESS_KEY_ID='YourAccessKeyValue' -e AWS_SECRET_ACCESS_KEY='YourSecretKeyValue'  treeverse/lakefs run --local-settings
   ```
Note using the ```bash--local-settings``` flag, metadata is being stored locally in the lakeFS container. Therefore, avoid using this flag for production usages.

## Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).
