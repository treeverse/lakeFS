---
layout: default
title: Launch the quickstart environment
description: TODO
parent: lakeFS Quickstart
nav_order: 1
has_children: false
# next: ["Create your first repository", "./repository.html"]
# redirect_from: [ "./quickstart/", "quickstart/installing.html", "quickstart/try.html"]
---

The quickstart uses Docker Compose to bring up the lakeFS container, pre-populate it with some data, and also provide a duckDB container from where we can interact with the data. 

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

You're now ready to dive into lakeFS! Login to lakeFS's web interface at http://127.0.0.1:8000 using these credentials:

* **Access Key ID**: `AKIA-EXAMPLE-KEY`
* **Secret Access Key**: `EXAMPLE-SECRET`

![](CleanShot%202023-03-21%20at%2015.50.39.png)
You'll see that there's a repository that's been created automagically for you, imaginatively called `quickstart`. Click on the repository name to open it.

![](CleanShot%202023-03-21%20at%2016.00.35.png)

Now we're ready to explore the data that's been loaded into the quickstart environment. 