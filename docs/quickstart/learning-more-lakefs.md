---
layout: default
title: 🧑🏻‍🎓 Learning more about lakeFS
description: TODO
parent: ⭐ Quickstart ⭐
nav_order: 99
has_children: false
---

# Learning more about lakeFS

The [lakeFS quickstart](/quickstart) is just the beginning of your lakeFS journey 🛣️

Here are some more resources to help you find out more about lakeFS. 

## Deploying lakeFS

The deployment guides show you how to deploy lakeFS [locally](deploy/onprem.html) (including on [Kubernetes](/deploy/onprem.html#k8s)) or on [AWS](/deploy/aws.html), [Azure](/deploy/azure.html), or [GCP](/deploy/gcp.html). 

Alternatively you might want to have a look at [lakeFS Cloud](https://lakefs.cloud/) which provides a fully-managed, SOC-2 compliant, lakeFS service. 

## lakeFS Samples

The [lakeFS Samples](https://github.com/treeverse/lakeFS-samples) GitHub repository includes some excellent examples including: 

* How to implement cross collection consistency on multiple Delta Tables
* Notebooks to show integration of lakeFS with Spark, Python, Delta Lake, Airflow and Hooks.
* Examples of using lakeFS webhooks to run automated data quality checks on different branches.
* Using lakeFS branching features to create dev/test data environments for ETL testing and experimentation.
* Reproducing ML experiments with certainty using lakeFS tags.

## lakeFS Community

lakeFS' community is important to us. Our **guiding principles** are: 

* Fully open, in code and conversation
* We learn and grow together
* Compassion and respect in every interaction

We'd love for you to join [our **Slack group**](https://lakefs.io/slack) and come and introduce yourself on `#say-hello`. Or just lurk and soak up the vibes 😎

If you're interested in getting involved in lakeFS' development head over our [the **GitHub repo**](https://github.com/treeverse/lakeFS) to look at the code and peruse the issues. The comprehensive [contributing](/contributing.html) document should have you covered on next steps but if you've any questions the `#dev` channel on [Slack](https://lakefs.io/slack) will be delighted to help. 

We love speaking at meetups and chatting to community members at them - you can find a list of these [here](https://lakefs.io/community/). 

Finally, make sure to drop by to say hi on [Twitter](https://twitter.com/lakeFS), [Mastodon](https://data-folks.masto.host/@lakeFS), and [LinkedIn](https://www.linkedin.com/company/treeverse/) 👋🏻

## lakeFS Concepts and Internals

We describe lakeFS as "_git for data_" but what does that actually mean? Have a look at the [concepts](/understand/model.html) and [architecture](/understand/architecture.html) guides, as well as the explanation of [how merges are handled](/understand/how/merge.html). To go deeper you might be interested in [the internals of versioning](/understand/how/versioning-internals.htm) and our [internal database structure](/understand/how/kv.html).