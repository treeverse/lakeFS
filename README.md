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
	<a href="https://artifacthub.io/packages/search?repo=lakefs">
		<img src="https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/lakefs" alt="Artifact HUB" /></a>
	<a href="CODE_OF_CONDUCT.md">
		<img src="https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg" alt="code of conduct"></a>
</p>


## lakeFS is a data version control - git for data

lakeFS is an open-source tool that transforms your object storage into a Git-like repository. It enables you to manage your data lake the way you manage your code.

With lakeFS you can build repeatable, atomic, and versioned data lake operations - from complex ETL jobs to data science and analytics.

lakeFS supports AWS S3, Azure Blob Storage, and Google Cloud Storage as its underlying storage service. It is API compatible with S3 and works seamlessly with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.


For more information, see the [official lakeFS documentation](https://docs.lakefs.io).


## Capabilities

### ETL Testing with Isolated Dev/Test Environment

When working with a data lake, it’s useful to have replicas of your production environment. These replicas allow you to test these ETLs and understand changes to your data without impacting the consumers of the production data.

Running ETL and transformation jobs directly in production without proper ETL Testing is a guaranteed way to have data issues flow into dashboards, ML models, and other consumers sooner or later. The most common approach to avoid making changes directly in production is to create and maintain multiple data environments and perform ETL testing on them. Dev environment to develop the data pipelines and test environment where pipeline changes are tested before pushing it to production

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

Everytime there is an update to production data, the best practice would be to run CI tests and then promote(deploy) the data to production.

### Rollback
A rollback operation is used to to fix critical data errors immediately.

What is a critical data error? Think of a situation where erroneous or misformatted data causes a signficant issue with an important service or function. In such situations, the first thing to do is stop the bleeding.

Rolling back returns data to a state in the past, before the error was present. You might not be showing all the latest data after a rollback, but at least you aren’t showing incorrect data or raising errors.

## Getting Started

### Using Docker

_Use this section to learn about lakeFS. For a production-suitable deployment, see the [docs](https://docs.lakefs.io/deploy/)._

1. Ensure you have Docker installed on your computer.

2. Run the following command:

   ```bash
   docker run --pull always --name lakefs -p 8000:8000 treeverse/lakefs run --local-settings
   ```

3. Open [http://127.0.0.1:8000/](http://127.0.0.1:8000/) in your web browser to set up an initial admin user.  You will use this user to log in and send API requests.

### Other quickstart methods

You can try lakeFS:

* Without installing, using the [lakeFS Playground](https://demo.lakefs.io).
* [On Kubernetes](https://docs.lakefs.io/quickstart/more_quickstart_options.html#on-kubernetes-with-helm).
* By [running the binary directly](https://docs.lakefs.io/quickstart/more_quickstart_options.html#using-the-binary).

### Setting up a repository

Once lakeFS is installed, you are ready to [create your first repository](https://docs.lakefs.io/quickstart/repository)!

## Community

Stay up to date and get lakeFS support via:

- Share your lakeFS experience and get support on [our Slack](https://go.lakefs.io/JoinSlack).
- Follow us and join the conversation on [Twitter](https://twitter.com/lakeFS).
- Learn from video tutorials on [our YouTube channel](https://lakefs.io/youtube).
- Read more on data versioning and other data lake best practices in [our blog](https://lakefs.io/blog/).
- Feel free to [contact us](https://lakefs.io/contact-us/) about anything else.

## More information

- Read the [documentation](https://docs.lakefs.io).
- See the [contributing guide](https://docs.lakefs.io/contributing).
- Take a look at our [roadmap](https://docs.lakefs.io/understand/roadmap.html) to peek into the future of lakeFS.

## Licensing

lakeFS is completely free and open-source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
