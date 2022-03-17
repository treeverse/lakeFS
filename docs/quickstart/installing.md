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

To run a local lakeFS instance using [Docker Compose](https://docs.docker.com/compose/){:target="_blank" class="button-clickable"}:

1. Ensure you have Docker and Docker Compose installed on your computer, and that Compose version is 1.25.04 or higher. For more information, please see this [issue](https://github.com/treeverse/lakeFS/issues/894){: class="button-clickable"}. 

1. Run the following command in your terminal:

   ```bash
   curl https://compose.lakefs.io | docker-compose -f - up
   ```

1. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank" class="button-clickable"} in your web browser.

1. [Create your first repository](repository.md){: .button-clickable} in lakeFS.

## Other methods

You can try lakeFS:

1. [On Kubernetes](more_quickstart_options.md#on-kubernetes-with-helm){: .button-clickable}.
1. With docker-compose [on Windows](more_quickstart_options.md#docker-on-windows){: .button-clickable}.
1. By [running the binary directly](more_quickstart_options.md#using-the-binary){: .button-clickable}.

## Next steps

Now that your lakeFS is running, try [creating a repository](repository.md){: .button-clickable}.
