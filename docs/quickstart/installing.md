---
layout: default
title: Install lakeFS
parent: Quick Start
nav_order: 10
has_children: false
---

# Installing lakeFS
{: .no_toc }

**Note** This section is for learning purposes. The installations below will not persist your data.
For a production suitable deployment, see [Deploying on AWS](../deployment/index.md).
{: .note }

## Using docker-compose
{: .no_toc }

If you wish to install your lakeFS using Kubernetes, install it manually, or install it on Windows, check out [Other Installations](other_installations.md) page.
{: .note .note-info }

To run a local lakeFS instance using [Docker Compose](https://docs.docker.com/compose/){:target="_blank"}:

1. Ensure you have Docker & Docker Compose installed on your computer.

1. Run the following command in your terminal:

   ```bash
   curl https://compose.lakefs.io | docker-compose -f - up
   ```

1. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"} in your web browser.

### Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).
