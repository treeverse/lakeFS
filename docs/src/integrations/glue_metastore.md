---
title: Glue Data Catalog
description: Set up AWS Glue Catalog Federation with lakeFS to expose Iceberg tables to Athena and other AWS analytics services.
status: enterprise
---

# Using lakeFS with the AWS Glue Data Catalog

!!! info
    Available in **lakeFS Enterprise**

!!! tip
    This integration requires the [lakeFS Iceberg REST Catalog](./iceberg.md) to be enabled.
    [Contact us](https://lakefs.io/lp/iceberg-rest-catalog/) to get started!

## Overview

Using [AWS Glue Catalog Federation](https://docs.aws.amazon.com/glue/latest/dg/catalog-federation.html), you can expose lakeFS-managed Apache Iceberg tables to any AWS service that reads from the Glue Data Catalog -- including [Amazon Athena](./athena.md), Redshift Spectrum, and EMR.

The [`lakefs-glue`](https://github.com/treeverse/lakefs-glue-federation) CLI tool automates the creation of a federated Glue catalog that connects directly to the lakeFS Iceberg REST Catalog. Table metadata is discovered in real time through lakeFS -- no data copying or metadata syncing required.

### How It Works

1. A query engine (e.g. Athena) submits a SQL query referencing a table in the federated **Glue Data Catalog**.
2. **Glue** connects to the **lakeFS Iceberg REST Catalog** via a REST API connection and retrieves table metadata.
3. **Lake Formation** issues temporary, scoped S3 credentials to the query engine.
4. The query engine reads Iceberg data files directly from **S3**.

### Prerequisites

- A lakeFS instance with the [Iceberg REST Catalog](./iceberg.md) enabled.
- A lakeFS service account (access key and secret key).
- Python 3.11+ (for running the `lakefs-glue` CLI).
- AWS credentials with permissions to manage IAM, Glue, Lake Formation, and Secrets Manager.

## Setting Up a Federated Catalog

### Install the CLI

=== "uv (recommended)"

    The easiest way to run the tool is with [`uv`](https://docs.astral.sh/uv/) -- no install needed:

    ```bash
    uvx lakefs-glue federate --help
    ```

=== "pip"

    ```bash
    pip install lakefs-glue
    lakefs-glue federate --help
    ```

### Create a Federated Catalog

Run the `federate` command to create a Glue federated catalog that points to a lakeFS repository and ref:

=== "uv"

    ```bash
    uvx lakefs-glue federate \
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \
        --lakefs-repo my-repo \
        --lakefs-access-key-id AKIAIOSFODNN7EXAMPLE \
        --lakefs-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    ```

=== "pip"

    ```bash
    lakefs-glue federate \
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \
        --lakefs-repo my-repo \
        --lakefs-access-key-id AKIAIOSFODNN7EXAMPLE \
        --lakefs-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    ```

This creates a federated catalog named `lakefs-catalog` (the default) pointing to the `main` branch.

### Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--lakefs-url` | Yes | | lakeFS server URL |
| `--lakefs-repo` | Yes | | Repository name |
| `--lakefs-ref` | No | `main` | Branch, tag, or commit ID to expose |
| `--lakefs-access-key-id` | Yes | | Service account access key |
| `--lakefs-secret-access-key` | Yes | | Service account secret key |
| `--catalog-name` | No | `lakefs-catalog` | Name for the Glue federated catalog |
| `--region` | No | `us-east-1` | AWS region |
| `--grant-to` | No | | IAM ARNs to grant catalog access to (repeatable) |

### What Gets Created

The `federate` command creates the following AWS resources:

- **Secrets Manager secret** (`<catalog-name>-secret`) -- stores lakeFS credentials for OAuth2 authentication.
- **IAM role** (`<catalog-name>-GlueConnectionRole`) -- assumed by Glue and Lake Formation.
- **Glue Connection** (`<catalog-name>-connection`) -- the REST API bridge to lakeFS.
- **Lake Formation resource** -- enables S3 credential vending.
- **Glue Catalog** (`<catalog-name>`) -- the federated catalog visible in Athena and other AWS services.
- **Lake Formation grants** -- permissions for any principals specified with `--grant-to`.

The command is idempotent: running it again with different parameters updates the existing resources.

## Working with Multiple Branches and Refs

Each federated catalog points to a single lakeFS ref. To query multiple branches, tags, or commits, create a separate catalog for each:

=== "uv"

    ```bash
    # Main branch
    uvx lakefs-glue federate \
        --lakefs-repo my-repo \
        --lakefs-ref main \
        --catalog-name my-repo-main \
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \
        --lakefs-access-key-id $LAKEFS_ACCESS_KEY_ID \
        --lakefs-secret-access-key $LAKEFS_SECRET_ACCESS_KEY

    # Development branch
    uvx lakefs-glue federate \
        --lakefs-repo my-repo \
        --lakefs-ref dev \
        --catalog-name my-repo-dev \
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \
        --lakefs-access-key-id $LAKEFS_ACCESS_KEY_ID \
        --lakefs-secret-access-key $LAKEFS_SECRET_ACCESS_KEY

    # A specific tag
    uvx lakefs-glue federate \
        --lakefs-repo my-repo \
        --lakefs-ref v1.0 \
        --catalog-name my-repo-v1 \
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \
        --lakefs-access-key-id $LAKEFS_ACCESS_KEY_ID \
        --lakefs-secret-access-key $LAKEFS_SECRET_ACCESS_KEY
    ```

=== "pip"

    ```bash
    # Main branch
    lakefs-glue federate \
        --lakefs-repo my-repo \
        --lakefs-ref main \
        --catalog-name my-repo-main \
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \
        --lakefs-access-key-id $LAKEFS_ACCESS_KEY_ID \
        --lakefs-secret-access-key $LAKEFS_SECRET_ACCESS_KEY

    # Development branch
    lakefs-glue federate \
        --lakefs-repo my-repo \
        --lakefs-ref dev \
        --catalog-name my-repo-dev \
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \
        --lakefs-access-key-id $LAKEFS_ACCESS_KEY_ID \
        --lakefs-secret-access-key $LAKEFS_SECRET_ACCESS_KEY

    # A specific tag
    lakefs-glue federate \
        --lakefs-repo my-repo \
        --lakefs-ref v1.0 \
        --catalog-name my-repo-v1 \
        --lakefs-url https://my-org.us-east-1.lakefscloud.io \
        --lakefs-access-key-id $LAKEFS_ACCESS_KEY_ID \
        --lakefs-secret-access-key $LAKEFS_SECRET_ACCESS_KEY
    ```

Each catalog appears independently in Athena and Lake Formation.

## Querying the Federated Catalog

Once set up, you can query your lakeFS tables from any AWS service that integrates with Glue Data Catalog. For a detailed guide with query examples, see [Using lakeFS with AWS Glue & Amazon Athena](./athena.md).

## Removing Federated Catalogs

To clean up federated catalogs and their associated AWS resources:

=== "uv"

    ```bash
    # Remove a specific catalog
    uvx lakefs-glue rm my-catalog

    # Remove all federated catalogs
    uvx lakefs-glue rm --all

    # Skip confirmation prompt
    uvx lakefs-glue rm my-catalog --yes

    # Target a specific region
    uvx lakefs-glue rm my-catalog --region us-west-2
    ```

=== "pip"

    ```bash
    # Remove a specific catalog
    lakefs-glue rm my-catalog

    # Remove all federated catalogs
    lakefs-glue rm --all

    # Skip confirmation prompt
    lakefs-glue rm my-catalog --yes

    # Target a specific region
    lakefs-glue rm my-catalog --region us-west-2
    ```

## Limitations

- **Read-only**: AWS Glue Catalog Federation only supports read queries. `INSERT`, `CREATE TABLE`, and other write operations are not supported.
- **Single ref per catalog**: Each federated catalog points to one lakeFS ref. Create multiple catalogs to query multiple branches or tags.
- **Flat namespaces only**: AWS Glue Catalog Federation supports only flat `catalog.namespace.table` structures -- nested namespaces are not supported.
