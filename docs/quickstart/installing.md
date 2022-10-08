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

### Update Container SSL Certificates
Run the following commands in your terminal to update the container SSL certificates.

1. Copy your SSL certificates from the local machine to the lakeFS container:

   ```bash
   docker cp <path_to_certificates_on_your_machine> <lakefs_container_name>:/usr/local/share/ca-certificates/
   ```

1. Update certificates inside the container:

   ```bash
   docker exec -it --user=root <lakefs_container_name> apk update 
   docker exec -it --user=root <lakefs_container_name> apk add ca-certificates 
   ```

1. Restart lakeFS container:

   ```bash
   docker restart <lakefs_container_name>
   ```

## Other methods

You can try lakeFS:

1. [On Kubernetes](more_quickstart_options.md#on-kubernetes-with-helm).
1. With docker-compose [on Windows](more_quickstart_options.md#docker-on-windows).
1. By [running the binary directly](more_quickstart_options.md#using-the-binary).

## Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).
