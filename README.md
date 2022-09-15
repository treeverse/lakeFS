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


## What is lakeFS

lakeFS is an open-source tool that transforms your object storage into a Git-like repository. It enables you to manage your data lake the way you manage your code.

With lakeFS you can build repeatable, atomic, and versioned data lake operations - from complex ETL jobs to data science and analytics.

lakeFS supports AWS S3, Azure Blob Storage, and Google Cloud Storage as its underlying storage service. It is API compatible with S3 and works seamlessly with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.


For more information, see the [official lakeFS documentation](https://docs.lakefs.io).


## Capabilities

### In Development

* **Experiment** - try new tools, upgrade versions, and evaluate code changes in isolation. You can get an isolated snapshot to run and compare experiments by creating a new branch of the data, while others are not exposed.
* **Debug** - checkout specific commits in a repository’s commit history to materialize consistent, historical versions of your data. 
* **Collaborate** - leverage isolated branches managed by metadata (not copies of files) to work in parallel.

### During Deployment

* **Version Control** - deploy data safely with CI/CD workflows borrowed from software engineering best practices. Ingest new data onto an isolated branch, perform data validations, then add to production through a merge operation.
* **Test** - define pre-merge and pre-commit hooks to run tests that enforce schema and validate properties of the data to catch issues before they reach production.

### In Production

* **Roll Back** - recover from errors by instantly reverting data to a former, consistent snapshot of the data lake. Choose any commit in a repository’s commit history to revert in one atomic action.
* **Troubleshoot** - investigate production errors by starting with a snapshot of the inputs to the failed process. 
* **Cross-collection Consistency** - provide consumers multiple synchronized collections of data in one atomic, revertable action. 


## Getting Started

### Using Docker

_Use this section to learn about lakeFS. For a production-suitable deployment, see the [docs](https://docs.lakefs.io/deploy/)._

1. Ensure you have Docker installed on your computer.

2. Run the following command:

   ```bash
   docker run --pull always --name lakefs -p 8000:8000 treeverse/lakefs
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
- Feel free to [contact us](https://lakefs.io/contact-us/) about anything else.

## More information

- Read the [documentation](https://docs.lakefs.io).
- See the [contributing guide](https://docs.lakefs.io/contributing).
- Take a look at our [roadmap](https://docs.lakefs.io/understand/roadmap.html) to peek into the future of lakeFS.

## Licensing

lakeFS is completely free and open-source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
