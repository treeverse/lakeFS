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
For a production suitable deployment, see [Deploying on AWS](../deploying/index.md).
{: .note }

## Using docker-compose
{: .no_toc }
If you wish to install your lakeFS using Kubernetes or just install it manually, check out [Other Installations](other_installations.md) page.
{: .note .note-info }

To run a local lakeFS instance, you can clone the repository and run [Docker Compose](https://docs.docker.com/compose/){:target="_blank"} application:

1. Ensure you have Docker installed on your computer. The MacOS and Windows installations include [Docker Compose](https://docs.docker.com/compose/){:target="_blank"} by default.

1. Clone the lakeFS repository:

   ```bash
   $ git clone https://github.com/treeverse/lakeFS
   ```

1. Navigate to the directory: `cd lakeFS`.

1. Run the following command:

   ```bash
   $ docker-compose up
   ```

1. Check your installation by opening [http://localhost:8000/setup](http://localhost:8000/setup){:target="_blank"} in your web browser.

### Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).
