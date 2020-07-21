# lakeFS


<p align="center">
  <img src="logo_large.png"/>
</p>

[![Go](https://github.com/treeverse/lakeFS/workflows/Go/badge.svg)](https://github.com/treeverse/lakeFS/actions?query=workflow%3AGo+branch%3Amaster)
[![Node](https://github.com/treeverse/lakeFS/workflows/Node/badge.svg)](https://github.com/treeverse/lakeFS/actions?query=workflow%3ANode+branch%3Amaster)

## What is lakeFS

An open source platform that empowers your object storage data lake with ACID guarantees and Git-like capabilities.


For more information see the [Official Documentation](https://docs.lakefs.io/).

## Capabilities

#### ACID Guarantees

Simplifying the workflow between data producers and consumers, providing strong isolation and enabling data consistency, integrity and availability

#### Data Versioning

Applying the power of branching, merging and reverting to your data lake.

#### Instant Revert

Revert back if something goes wrong with your data. Increasing developer velocity while ensuring production safety and stability.

#### Data Deduplication

Reduce the impact of redundant data on your storage manageability and governance.


## Getting Started

#### Docker

1. Ensure you have Docker installed on your computer. The MacOS and Windows installations include Docker Compose by default.

2. Create a `docker-compose.yaml` file, containing the following configuration:
    ```yaml
    ---
    version: '3'
    services:
      lakefs:
        image: "treeverse/lakefs:latest"
        ports: ["8000:8000"]
        links: ["postgres"]
        environment:
          LAKEFS_AUTH_ENCRYPT_SECRET_KEY: "some random secret string"
          LAKEFS_DATABASE_CONNECTION_STRING: postgres://lakefs:lakefs@postgres/postgres?sslmode=disable
          LAKEFS_BLOCKSTORE_TYPE: local
          LAKEFS_BLOCKSTORE_LOCAL_PATH: /home/lakefs
        entrypoint: ["/app/wait-for", "postgres:5432", "--", "/app/lakefs", "run"]
      postgres:
        image: "postgres:11"
        environment:
          POSTGRES_USER: lakefs
          POSTGRES_PASSWORD: lakefs
    ```

3. From the directory that contains our new docker-compose.yaml file, run the following command:

   ```bash
   $ docker-compose up
   ```

4. Open [http://localhost:8000/setup](http://localhost:8000/setup) in your web browser to set up an initial admin user, used to login and send API requests.

#### Download the Binary

Alternatively, you can download the lakeFS binaries and run them directly.

Binaries are available at [https://releases.lakefs.io](https://releases.lakefs.io).


## Getting started

Please follow the [Guide to Get Started](https://docs.lakefs.io/quickstart.html#setting-up-a-repository) to set up your local lakeFS installation.


## Community

Keep up to date and get lakeFS support via:

- Join our [Community Slack Channel](https://join.slack.com/t/lakefs/shared_invite/zt-fm6e2ncx-6wR3yW5jABXBuqN2NnLCDA) to get help from our team and other users.
- Follow us on [Twitter](https://twitter.com/lake_FS).
- [Contact us](mailto:hello@treeverse.io)

## More information

- [lakeFS documentation](https://docs.lakefs.io/)
- If you would like to contribute, check out our [contributing guide](https://docs.lakefs.io/contributing/).

## Licensing

lakeFS is completely free and open source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

