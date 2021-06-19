---
layout: default
title: Install lakeFS
description: >-
  Installing lakeFS is easy. This section covers how to install lakeFS using
  docker compose.
parent: Quickstart
nav_order: 10
has_children: false
---

# Install lakeFS

{: .no\_toc }

## Using docker-compose

{: .no\_toc }

Other quickstart methods can be found [here](more_quickstart_options.md). {: .note .note-info }

To run a local lakeFS instance using [Docker Compose](https://docs.docker.com/compose/){:target="\_blank"}:

1. Ensure you have Docker & Docker Compose installed on your computer, and that compose version is 1.25.04 or higher. For more information, please see this [issue](https://github.com/treeverse/lakeFS/issues/894).
2. Run the following command in your terminal:

   ```bash
   curl https://compose.lakefs.io | docker-compose -f - up
   ```

3. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="\_blank"} in your web browser.

### Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).

