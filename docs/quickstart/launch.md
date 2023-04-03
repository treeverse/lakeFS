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

_The quickstart uses Docker Compose to bring up the lakeFS container, pre-populate it with some data, and also provide a DuckDB container from where we can interact with the data. You'll need [Docker](https://docs.docker.com/get-docker/) and [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) installed to run this._

The first step is to clone the lakeFS git repository:

```bash
git clone git@github.com:treeverse/lakeFS.git
cd lakeFS/quickstart
```

and then launch the environment with Docker Compose:

```bash
docker-compose up
```

After a few moments you should see the lakeFS container ready to use: 

```
[…]
lakefs  | -------- Let's go and have axolotl fun! --------
lakefs  |
lakefs  | >(.＿.)<     http://127.0.0.1:8000/
lakefs  |   (  )_
lakefs  |              Access Key ID    : AKIA-EXAMPLE-KEY
lakefs  |              Secret Access Key: EXAMPLE-SECRET
lakefs  |
lakefs  | ------------------------------------------------
```

You're now ready to dive into lakeFS! 

Login to lakeFS's web interface at [http://127.0.0.1:8000](http://127.0.0.1:8000) using these credentials:

* **Access Key ID**: `AKIA-EXAMPLE-KEY`
* **Secret Access Key**: `EXAMPLE-SECRET`

<img src="/assets/img/quickstart/lakefs-login-screen.png" alt="The lakeFS login screen" class="quickstart"/>

You'll see that there's a repository that's been created automagically for you, imaginatively called `quickstart`. Click on the repository name to open it.

<img src="/assets/img/quickstart/repo-list.png" alt="A list of repositories in lakeFS" class="quickstart"/>

Now we're ready to explore the data that's been loaded into the quickstart environment. 