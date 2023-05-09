---
title: 1️⃣ Run lakeFS
description: lakeFS quickstart / Run lakeFS locally pre-populated with a sample repository and data under Docker Compose
parent: ⭐ Quickstart ⭐
nav_order: 5
has_children: false
next: ["Query the pre-populated data", "./query.html"]
previous: ["Quickstart introduction", "./index.html"]
---

# 👩🏻‍💻 Spin up the environment 👨🏻‍💻

_The quickstart uses Docker to bring up the lakeFS container, pre-populate it with some data, and also provides DuckDB from where we can interact with the data. You'll need [Docker](https://docs.docker.com/get-docker/) installed to run this._

Launch the lakeFS container:

```bash
docker run --name lakefs \
           --rm --publish 8000:8000 \
           --entrypoint "/bin/sh" \
           --env "LAKECTL_CREDENTIALS_ACCESS_KEY_ID=AKIA-EXAMPLE-KEY" \
           --env "LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=EXAMPLE-SECRET" \
           --env "LAKECTL_SERVER_ENDPOINT_URL=http://localhost:8000" \
           treeverse/lakefs:latest -c \
           'lakefs setup --local-settings --user-name admin --access-key-id "$LAKECTL_CREDENTIALS_ACCESS_KEY_ID" --secret-access-key "$LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"; \ 
            lakefs run --local-settings'
```

After a few moments you should see the lakeFS container ready to use: 

```
[…]
│
│ If you're running lakeFS locally for the first time,
│     complete the setup process at http://127.0.0.1:8000/setup
│
[…]

```

You're now ready to dive into lakeFS! 

1. Login to lakeFS's web interface at [http://127.0.0.1:8000/](http://127.0.0.1:8000/) using these credentials:

    * **Access Key ID**: `AKIA-EXAMPLE-KEY`
    * **Secret Access Key**: `EXAMPLE-SECRET`

2. You'll notice that there aren't any repositories created yet. Click the **Create Sample Repository** button. 

    <img width="75%" src="/assets/img/quickstart/empty-repo-list.png" alt="Empty lakeFS Repository list" class="quickstart"/>

You will see the sample repository created and the quickstart guide within it. You can follow along there, or here - it's the same :) 

<img width="75%" src="/assets/img/quickstart/quickstart-repo.png" alt="The quickstart sample repo in lakeFS" class="quickstart"/>
