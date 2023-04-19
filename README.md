<p align="center">
  <img src="docs/assets/img/logo_large.png"/>
</p>
<p align="center">
	<a href="https://raw.githubusercontent.com/treeverse/lakeFS/master/LICENSE" >
		<img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache License" /></a>
	<a href="https://github.com/treeverse/lakeFS/actions?query=workflow%3AGo+branch%3Amaster++">
		<img src="https://github.com/treeverse/lakeFS/workflows/Go/badge.svg?branch=master" alt="Go tests status" /></a>
	<a href="https://github.com/treeverse/lakeFS/actions?query=workflow%3ANode+branch%3Amaster++" >
		<img src="https://github.com/treeverse/lakeFS/workflows/Node/badge.svg?branch=master" alt="Node tests status" /></a>
	<a href="https://github.com/treeverse/lakeFS/actions?query=workflow%3AEsti">
		<img src="https://github.com/treeverse/lakeFS/workflows/Esti/badge.svg" alt="Integration tests status" /></a>
	<a href="https://github.com/treeverse/lakeFS/actions/workflows/docs-pr.yaml">
		<img src="https://github.com/treeverse/lakeFS/actions/workflows/docs-pr.yaml/badge.svg" alt="Docs Preview & Link Check status" /></a>
	<a href="https://artifacthub.io/packages/search?repo=lakefs">
		<img src="https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/lakefs" alt="Artifact HUB" /></a>
	<a href="CODE_OF_CONDUCT.md">
		<img src="https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg" alt="code of conduct"></a>
</p>


## lakeFS is Data Version Control (Git for Data)

lakeFS is an open-source tool that transforms your object storage into a Git-like repository. It enables you to manage your data lake the way you manage your code.

With lakeFS you can build repeatable, atomic, and versioned data lake operations - from complex ETL jobs to data science and analytics.

lakeFS supports AWS S3, Azure Blob Storage, and Google Cloud Storage as its underlying storage service. It is API compatible with S3 and works seamlessly with all modern data frameworks such as Spark, Hive, AWS Athena, DuckDB, and Presto.

For more information, see the [documentation](https://docs.lakefs.io).

## Getting Started

You can spin up a standalone sandbox instance of lakeFS quickly under Docker by running the following:

```bash
docker run --pull always \
		   --name lakefs \
		   -p 8000:8000 \
		   treeverse/lakefs:latest \
		   run --local-settings
```

Once you've got lakeFS running, open [http://127.0.0.1:8000/](http://127.0.0.1:8000/) in your web browser.

### Quickstart

**👉🏻 For a hands-on walk through of the core functionality in lakeFS head over to [the quickstart](https://docs.lakefs.io/quickstart/) to jump right in!**

Make sure to also have a look at the [lakeFS samples](https://github.com/treeverse/lakeFS-samples). These are a rich resource of examples of end-to-end applications that you can build with lakeFS.

## Why Do I Need lakeFS?

### ETL Testing with Isolated Dev/Test Environment

When working with a data lake, it’s useful to have replicas of your production environment. These replicas allow you to test these ETLs and understand changes to your data without impacting downstream data consumers.

Running ETL and transformation jobs directly in production without proper ETL Testing is a guaranteed way to have data issues flow into dashboards, ML models, and other consumers sooner or later. The most common approach to avoid making changes directly in production is to create and maintain multiple data environments and perform ETL testing on them. Dev environment to develop the data pipelines and test environment where pipeline changes are tested before pushing it to production. With lakeFS you can create branches, and get a copy of the full production data, without copying anything. This enables a faster and easier process of ETL testing.

### Reproducibility

Data changes frequently. This makes the task of keeping track of its exact state over time difficult. Oftentimes, people maintain only one state of their data––its current state.

This has a negative impact on the work, as it becomes hard to:
* Debug a data issue.
* Validate machine learning training accuracy (re-running a model over different data gives different results).
Comply with data audits.

In comparison, lakeFS exposes a Git-like interface to data that allows keeping track of more than just the current state of data. This makes reproducing its state at any point in time straightforward.

### CI/CD for Data

Data pipelines feed processed data from data lakes to downstream consumers like business dashboards and machine learning models. As more and more organizations rely on data to enable business critical decisions, data reliability and trust are of paramount concern. Thus, it’s important to ensure that production data adheres to the data governance policies of businesses. These data governance requirements can be as simple as a file format validation, schema check, or an exhaustive PII(Personally Identifiable Information) data removal from all of organization’s data.

Thus, to ensure the quality and reliability at each stage of the data lifecycle, data quality gates need to be implemented. That is, we need to run Continuous Integration(CI) tests on the data, and only if data governance requirements are met can the data can be promoted to production for business use.

Everytime there is an update to production data, the best practice would be to run CI tests and then promote(deploy) the data to production. With lakeFS you can create hooks that make sure that only data that passed these tests will become part of production.

### Rollback
A rollback operation is used to to fix critical data errors immediately.

What is a critical data error? Think of a situation where erroneous or misformatted data causes a signficant issue with an important service or function. In such situations, the first thing to do is stop the bleeding.

Rolling back returns data to a state in the past, before the error was present. You might not be showing all the latest data after a rollback, but at least you aren’t showing incorrect data or raising errors. Since lakeFS provides versions of the data without making copies of the data, you can time travel between versions and roll back to the version of the data before the error was presented.

## Community

Stay up to date and get lakeFS support via:

- Share your lakeFS experience and get support on [our Slack](https://go.lakefs.io/JoinSlack).
- Follow us and join the conversation on [Twitter](https://twitter.com/lakeFS) and [Mastodon](https://data-folks.masto.host/@lakeFS).
- Learn from video tutorials on [our YouTube channel](https://lakefs.io/youtube).
- Read more on data versioning and other data lake best practices in [our blog](https://lakefs.io/blog/data-version-control/).
- Feel free to [contact us](https://lakefs.io/contact-us/) about anything else.

## More information

- Read the [documentation](https://docs.lakefs.io).
- See the [contributing guide](https://docs.lakefs.io/contributing).
- Take a look at our [roadmap](https://docs.lakefs.io/understand/roadmap.html) to peek into the future of lakeFS.

## Licensing

lakeFS is completely free and open-source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
