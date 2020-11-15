---
layout: default
title: Install lakeFS
parent: Quick Start
nav_order: 10
has_children: false
---

# Installing lakeFS
{: .no_toc }

{% include learn_only.html %} 

## Using docker-compose
{: .no_toc }

Other quickstart methods can be found [here](more_quickstart_options.md).
{: .note .note-info }

To run a local lakeFS instance using [Docker Compose](https://docs.docker.com/compose/){:target="_blank"}:

1. Ensure you have Docker & Docker Compose installed on your computer, and that compose version is 1.25.04 or higher. For more information, please see this [issue](https://github.com/treeverse/lakeFS/issues/894). 

1. Run the following command in your terminal:

   ```bash
   curl https://compose.lakefs.io | docker-compose -f - up
   ```

1. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"} in your web browser.

### Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).
