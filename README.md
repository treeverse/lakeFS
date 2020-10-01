<p align="center">
  <img src="logo_large.png"/>
</p>

[![Hacktoberfest](https://badgen.net/badge/hacktoberfest/friendly/pink)](docs/contributing.md)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://raw.githubusercontent.com/treeverse/lakeFS/master/LICENSE)
[![Go](https://github.com/treeverse/lakeFS/workflows/Go/badge.svg?branch=master)](https://github.com/treeverse/lakeFS/actions?query=workflow%3AGo+branch%3Amaster++)
[![Node](https://github.com/treeverse/lakeFS/workflows/Node/badge.svg?branch=master)](https://github.com/treeverse/lakeFS/actions?query=workflow%3ANode+branch%3Amaster++)


## What is lakeFS

lakeFS is an open source layer that delivers resilience and manageability to object-storage based data lakes.

With lakeFS you can build repeatable, atomic and versioned data lake operations - from complex ETL jobs to data science and analytics.

lakeFS supports AWS S3 or Google Cloud Storage as its underlying storage service. It is API compatible with S3, and works seamlessly with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.


<p align="center">
  <img src="docs/assets/img/wrapper.png"/>
</p>

For more information see the [Official Documentation](https://docs.lakefs.io).


## Capabilities

**Development Environment for Data**
* **Experimentation** - Try tools, upgrade versions and evaluate code changes in isolation. 
* **Reproducibility** - Go back to any point of time to a consistent version of your data lake.

**Continuous Data Integration**
* **Ingest new data safely by enforcing best practices** - Make sure new data sources adhere to your lake’s best practices such as format and schema enforcement, naming convention, etc.  
* **Metadata validation** - Prevent breaking changes from entering the production data environment.

**Continuous Data Deployment**
* **Instantly revert changes to data** - If low quality data is exposed to your consumers, you can revert instantly to a former, consistent and correct snapshot of your data lake.
* **Enforce cross collection consistency** - Provides the consumers several collections of data that must be synchronized, in one atomic, revertible, action.
* **Prevent data quality issues by enabling**
  - Testing of production data before exposing it to users / consumers.
  - Testing of intermediate results in your DAG to avoid cascading quality issues.

## Getting Started

#### Docker (MacOS, Linux)

1. Ensure you have Docker & Docker Compose installed on your computer.

2. Run the following command:

   ```bash
   curl https://compose.lakefs.io | docker-compose -f - up
   ```

3. Open [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup) in your web browser to set up an initial admin user, used to login and send API requests.


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

For more detailed information on how to set up lakeFS, please visit [the documentation](https://docs.lakefs.io).

## Community

Stay up to date and get lakeFS support via:

- [Slack](https://join.slack.com/t/lakefs/shared_invite/zt-g86mkroy-186GzaxR4xOar1i1Us0bzw) (to get help from our team and other users).
- [Twitter](https://twitter.com/lakeFS) (follow for updates and news)
- [YouTube](https://www.youtube.com/channel/UCZiDUd28ex47BTLuehb1qSA) (learn from video tutorials)
- [Contact us](https://lakefs.io/contact-us/) (for anything)


## More information

- [lakeFS documentation](https://docs.lakefs.io)
- If you would like to contribute, check out our [contributing guide](https://docs.lakefs.io/contributing).

## Licensing

lakeFS is completely free and open source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

