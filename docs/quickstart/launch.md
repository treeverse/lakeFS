---
title: 1️⃣ Run lakeFS
description: lakeFS quickstart / Run lakeFS locally pre-populated with a sample repository
parent: ⭐ Quickstart
nav_order: 5
next: ["Query the pre-populated data", "./query.html"]
previous: ["Quickstart introduction", "./index.html"]
---

# Spin up the environment

{: .note}
If you don't want to install lakeFS locally, you can use the [30-day free trial of lakeFS Cloud](https://lakefs.cloud/register). Once you launch the free trial you will have access to the same content as this quickstart within the provided repository once you login.

install and launch lakeFS:

```bash
pip install lakefs
python -m lakefs.download
lakefs run --quickstart
```

{: .note }
If for some reason you get a command not found error, you can run `python -m lakefs run --quickstart` instead

After a few moments you should see the lakeFS container ready to use: 

```
│
│ lakeFS running in quickstart mode.
│     Login at http://127.0.0.1:8000/
│
│     Access Key ID    : AKIAIOSFOLQUICKSTART
│     Secret Access Key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
│
```


You're now ready to dive into lakeFS! 

1. Open lakeFS's web interface at [http://127.0.0.1:8000/](http://127.0.0.1:8000/)

2. Login with the quickstart credentials. 

    * Access Key ID: `AKIAIOSFOLQUICKSTART`
    * Secret Access Key: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`

3. You'll notice that there aren't any repositories created yet. Click the **Create Sample Repository** button. 

    <img width="75%" src="{{ site.baseurl }}/assets/img/quickstart/empty-repo-list.png" alt="Empty lakeFS Repository list" class="quickstart"/>

You will see the sample repository created and the quickstart guide within it. You can follow along there, or here - it's the same :) 

<img width="75%" src="{{ site.baseurl }}/assets/img/quickstart/quickstart-repo.gif" alt="The quickstart sample repo in lakeFS" class="quickstart"/>
