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
           treeverse/lakefs:latest \
           run --local-settings
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

Login to lakeFS's web interface at [http://127.0.0.1:8000](http://127.0.0.1:8000). Enter your email address and click on **Setup**

<img src="/assets/img/quickstart/user_config.png" alt="User Configuration Screen" class="quickstart"/>

On the next screen you'll see the generated authentication key pair - make a note of these (or download them using the button provided). 

Click on **Go To Login** and sign in using your new credentials. 

You notice that there aren't any repositories created yet. Click the **Create Sample Repository** button. 

<img src="/assets/img/quickstart/empty-repo-list.png" alt="Empty lakeFS Repository list" class="quickstart"/>

Specify `quickstart` as the repository name, leave the other settings unchanged, and click **Create Repository**

<img src="/assets/img/quickstart/create-quickstart-repo.png" alt="Creating the quickstart sample repo in lakeFS" class="quickstart"/>

You will see the sample repository created and the quickstart guide within it. You can follow along there, or here - it's the same :) 

<img src="/assets/img/quickstart/quickstart-repo.png" alt="The quickstart sample repo in lakeFS" class="quickstart"/>

Now we're ready to explore the data that's been loaded into the quickstart environment. 