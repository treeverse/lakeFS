<p align="center">
  <img src="logo_large.png"/>
</p>

[![Go](https://github.com/treeverse/lakeFS/workflows/Go/badge.svg?branch=master)](https://github.com/treeverse/lakeFS/actions?query=workflow%3AGo+branch%3Amaster++)
[![Node](https://github.com/treeverse/lakeFS/workflows/Node/badge.svg?branch=master)](https://github.com/treeverse/lakeFS/actions?query=workflow%3ANode+branch%3Amaster++)

## What is lakeFS

lakeFS is an open source layer that delivers resilience and manageability to object-storage based data lakes.

With lakeFS you can build repeatable, atomic and versioned data lake operations - from complex ETL jobs to data science and analytics.

lakeFS is API compatible with AWS S3 and works seamlessly with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.


<p align="center">
  <img src="docs/assets/img/wrapper.png"/>
</p>

For more information see the [Official Documentation](https://docs.lakefs.io).

## Capabilities

**Atomic Operations** - lakeFS allows data producers to manipulate multiple objects as a single, atomic operation. If something fails half-way, all changes can be instantly rolled back.

**Consistency** - lakeFS enables object-level and cross-collection consistency:
  - object-level consistency ensures all operations within a branch are strongly consistent (read-after-write, list-after-write, read-after-delete, etc).
  - cross-collection consistency is achieved by providing snapshot isolation. Using branches, writers can provide consistency guarantees across different logical   collections - merging to “main” is only done after several datasets have been created successfully.

**History** - Commits are retained for a configurable duration, so readers can query data from the latest commit, or from any other point in time. Writers can atomically and safely rollback changes to previous versions.

**Cross-Lake Isolation** - Creating a lakeFS branch provides you with a snapshot of the entire lake at a given point in time.
All reads from that branch are    guaranteed to always return the same results.

**Data CI/CD** - The ability to define automated rules and tests that are required to pass before committing or merging changes to data.


## Getting Started

#### Docker

1. Ensure you have Docker installed on your computer. The MacOS and Windows installations include Docker Compose by default.

2. Clone the repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS
   ```

3. From the root of the cloned repository, run:

   ```bash
   $ docker-compose up
   ```

4. Open [http://localhost:8000/setup](http://localhost:8000/setup) in your web browser to set up an initial admin user, used to login and send API requests.

#### Download the Binary

Alternatively, you can download the lakeFS binaries and run them directly.

Binaries are available at [https://github.com/treeverse/lakeFS/releases](https://github.com/treeverse/lakeFS/releases).


#### Setting up a repository

Please follow the [Guide to Get Started](https://docs.lakefs.io/quickstart.html#setting-up-a-repository) to set up your local lakeFS installation.

For more detailed inforamation on how to setup lakeFS, please visit [the documentation](https://docs.lakefs.io)

## Community

Keep up to date and get lakeFS support via:

- [Slack](https://join.slack.com/t/lakefs/shared_invite/zt-g86mkroy-186GzaxR4xOar1i1Us0bzw) (to get help from our team and other users).
- [Twitter](https://twitter.com/lakeFS) (follow for updates and news)
- [YouTube](https://www.youtube.com/channel/UCZiDUd28ex47BTLuehb1qSA) (learn from video tutorials)
- [Contact us](https://lakefs.io/contact-us/) (for anything)

## More information

- [lakeFS documentation](https://docs.lakefs.io)
- If you would like to contribute, check out our [contributing guide](https://docs.lakefs.io/contributing).

## Licensing

lakeFS is completely free and open source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

