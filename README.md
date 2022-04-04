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

**In Development**

* **Experiment** - try new tools, upgrade versions, and evaluate code changes in isolation. You can get an isolated snapshot to run and compare experiments by creating a new branch of the data, while others are not exposed.
* **Debug** - checkout specific commits in a repository’s commit history to materialize consistent, historical versions of your data. 
* **Collaborate** - leverage isolated branches managed by metadata (not copies of files) to work in parallel.

**During Deployment**

* **Version Control** - deploy data safely with CI/CD workflows borrowed from software engineering best practices. Ingest new data onto an isolated branch, perform data validations, then add to production through a merge operation.
* **Test** - define pre-merge and pre-commit hooks to run tests that enforce schema and validate properties of the data to catch issues before they reach production.

**In Production**

* **Roll Back** - recover from errors by instantly reverting data to a former, consistent snapshot of the data lake. Choose any commit in a repository’s commit history to revert in one atomic action.
* **Troubleshoot** - investigate production errors by starting with a snapshot of the inputs to the failed process. 
* **Cross-collection Consistency** - provide consumers multiple synchronized collections of data in one atomic, revertable action. 



## Getting Started

#### Docker (MacOS, Linux)

1. Ensure you have Docker & Docker Compose installed on your computer.

2. Run the following command:

   ```bash
   curl https://compose.lakefs.io | docker-compose -f - up
   ```

3. Open [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup) in your web browser to set up an initial admin user.  You will use this user to log in and send API requests.


#### Docker (Windows)

1. Ensure you have Docker installed.

2. Run the following command in PowerShell:

   ```shell script
   Invoke-WebRequest https://compose.lakefs.io | Select-Object -ExpandProperty Content | docker-compose -f - up
   ``` 

3. Open [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup) in your web browser to set up an initial admin user, used to login and send API requests.

#### Download the Binary

Alternatively, you can download the lakeFS binaries and run them directly.

Binaries are available at [https://github.com/treeverse/lakeFS/releases](https://github.com/treeverse/lakeFS/releases).


#### Setting up a repository

Please follow the [Guide to Get Started](https://docs.lakefs.io/quickstart/repository) to set up your local lakeFS installation.

For more detailed information on how to set up lakeFS, please visit the [lakeFS documentation](https://docs.lakefs.io).

## Community

Stay up to date and get lakeFS support via:

- [Slack](https://lakefs.io/slack) (to get help from our team and other users).
- [Twitter](https://twitter.com/lakeFS) (follow for updates and news)
- [YouTube](https://lakefs.io/youtube) (learn from video tutorials)
- [Contact us](https://lakefs.io/contact-us/) (for anything)

## More information

- [lakeFS documentation](https://docs.lakefs.io)
- If you would like to contribute, check out our [contributing guide](https://docs.lakefs.io/contributing).
- [Roadmap](https://docs.lakefs.io/understand/roadmap.html)

## Licensing

lakeFS is completely free and open-source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
