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

{: .note}
If you don't want to use Docker, you can use the [30-day free trial of lakeFS Cloud](https://lakefs.cloud/register). Once you launch the free trial you will have access to the same content as this quickstart within the provided repository once you login.

_The quickstart uses Docker to bring up the lakeFS container, pre-populate it with some data, and also provides DuckDB from where we can interact with the data. You'll need [Docker](https://docs.docker.com/get-docker/) installed to run this._

Launch the lakeFS container:

```bash
docker run --name lakefs --pull always \
             --rm --publish 8000:8000 \
             treeverse/lakefs:latest \
             run --quickstart
```

Quickstart mode includes initial setup and lakeFS is ready for use!
The lakeFS quickstart credentials are:

```
│     Access Key ID    : "AKIAIOSFOLQUICKSTART"
│     Secret Access Key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

You're now ready to dive into lakeFS! 

1. Open lakeFS's web interface at [http://127.0.0.1:8000/](http://127.0.0.1:8000/), enter your email address, and then click **Go to Login**.

2. Login with the quickstart credentials. 

3. You'll notice that there aren't any repositories created yet. Click the **Create Sample Repository** button. 

    <img width="75%" src="/assets/img/quickstart/empty-repo-list.png" alt="Empty lakeFS Repository list" class="quickstart"/>

You will see the sample repository created and the quickstart guide within it. You can follow along there, or here - it's the same :) 

<img width="75%" src="/assets/img/quickstart/quickstart-repo.gif" alt="The quickstart sample repo in lakeFS" class="quickstart"/>
