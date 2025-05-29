---
title: lakeFS Tables
description: Use lakeFS to manage Iceberg Tables using a builtin Iceberg REST Catalog
parent: How-To
---

# lakeFS Tables

lakeFS Enterprise
{: .label .label-purple }

{: .note}
> lakeFS Tables are currently in private preview for [lakeFS Enterprise]({% link enterprise/index.md %}) customers.
> [Contact us](https://lakefs.io/book-a-demo/) to get started!

{% include toc.html %}

## What are lakeFS Tables?

lakeFS Tables allow you to use lakeFS as a [spec-compliant](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) Apache [Iceberg REST catalog](https://www.tabular.io/apache-iceberg-cookbook/getting-started-catalog-background/), 
allowing Iceberg clients to manage Iceberg tables using a standard REST API. 

Using lakeFS Tables, you can use lakeFS a drop-in replacement for other Iceberg catalogs like AWS Glue, Nessie, or Hive Metastore.

With lakeFS Tables, you can:

- Manage Iceberg tables with full version control capabilities.
- Use standard Iceberg clients and tools without modification.
- Leverage lakeFS's branching and merging features for managing table's lifecycle.
- Maintain data consistency across different environments.

## Use Cases

1. **Version-Controlled Data Development**:
   - Create feature branches for table schema changes or data migrations
   - Test modifications in isolation, across multiple tables
   - Merge changes safely with conflict detection

2. **Multi-Environment Management**:
   - Use branches to represent different environments (dev, staging, prod)
   - Promote changes between environments through merges, with automated testing
   - Maintain consistent table schemas across environments

3. **Collaborative Data Development**:
   - Multiple teams can work on different table features simultaneously
   - Maintain data quality through pre-merge validations
   - Collaborate using [pull requests](./pull-requests.html) on changes to data and schema

4. **Manage and Govern Access to data**:
   - Use the detailed built-in commit log capturing who, what and how data is changed
   - Manage access using fine grained access control to users and groups using RBAC policies
   - Rollback changes atomically and safely to reduce time-to-recover and increase system stability

## Configuration

The Iceberg REST catalog API is exposed at `/iceberg/api` in your lakeFS server. 

To use it:

1. Enable the feature ([contact us](https://info.lakefs.io/thanks-iceberg-catalog) for details).
2. Configure your Iceberg clients to use the lakeFS REST catalog endpoint.
3. Use your lakeFS access key and secret for authentication.

#### Catalog Initialization Example (using [`pyiceberg`](https://py.iceberg.apache.org/))

```python
from pyiceberg.catalog import load_catalog

catalog = RestCatalog(**{
   'uri': f'{lakefs_endpoint}/iceberg/api',
   'oauth2-server-uri': f'{lakefs_endpoint}/iceberg/api/v1/oauth/tokens',
   'credential': f'{lakefs_client_key}:{lakefs_client_secret}',
})
```


### Example Client code

<div class="tabs">
  <ul>
    <li><a href="#python">Python</a></li>
    <li><a href="#trino">Trino</a></li>
    <li><a href="#spark">Spark</a></li>
    <li><a href="#starrocks">StarRocks</a></li>
  </ul>

  <div markdown="1" id="python">

```python
import lakefs
from pyiceberg.catalog import load_catalog

# Initialize the catalog
catalog = RestCatalog(**{
   'uri': 'https://lakefs.example.com/iceberg/api',
   'oauth2-server-uri': 'https://lakefs.example.com/iceberg/api/iceberg/api/v1/oauth/tokens',
   'credential': f'AKIAlakefs12345EXAMPLE:abc/lakefs/1234567bPxRfiCYEXAMPLEKEY',
})

# List namespaces in a branch
catalog.list_namespaces(('repo', 'main'))

# Query a table
catalog.list_tables('repo.main.inventory')
table = catalog.load_table('repo.main.inventory.books')
arrow_df = table.scan().to_arrow()
```

  </div>

  <div markdown="2" id="trino">

```sql
-- List tables in the iceberg catalog
USE "repo.main.inventory"; -- <repository>.<branch or reference>.<namespace>
SHOW TABLES;

-- Query a table
SELECT * FROM books LIMIT 100;

-- Switch to a different branch
USE "repo.new_branch.inventory";
SELECT * FROM books;
```

  </div>

  <div markdown="3" id="spark">

```scala
// Configure Spark to use the lakeFS REST catalog
spark.sql("USE my_repo.main.namespace")

// List available tables
spark.sql("SHOW TABLES").show()

// Query data with branch isolation
spark.sql("SELECT * FROM my_table").show()

// Switch to a feature branch
spark.sql("USE my_repo.new_branch.namespace")
spark.sql("SELECT * FROM my_table").show()
```

  </div>

  <div markdown="4" id="starrocks">

```sql
CREATE EXTERNAL CATALOG lakefs
COMMENT "lakeFS REST Catalog"
PROPERTIES (
    "type"                          = "iceberg",
    "iceberg.catalog.type"          = "rest",
    "iceberg.catalog.uri"           = "https://lakefs.example.com/catalog/iceberg/v1",
    "client.factory"                = "com.starrocks.connector.iceberg.IcebergAwsClientFactory"
);

-- Use a specific repository and branch
SELECT * FROM frosty.`repo_name.main.namespace`.table_name;
```

  </div>
</div>

## Namespaces and Tables

### Namespace Operations

The Iceberg Catalog supports Iceberg namespace operations:

- Create namespaces
- List namespaces
- Drop namespaces
- List tables within namespaces

#### Namespace Usage

Namespaces in the Iceberg Catalog follow the pattern `"<repository>.<branch>.<namespace>(.<namespace>...)"` where:

- `<repository>` must be a valid lakeFS repository name.
- `<branch>` must be a valid lakeFS branch name.
- `<namespace>` components can be nested using unit separator (e.g., `inventory.books`).

Examples:
- `my-repo.main.inventory`
- `my-repo.feature-branch.inventory.books`

The repository and branch components must already exist in lakeFS before using them in the Iceberg catalog.

#### Namespace Restrictions

- Repository and branch names must follow lakeFS naming conventions.
- Namespace components cannot contain special characters except dots (.) for nesting.
- The total namespace path length must be less than 255 characters.
- Namespaces are case-sensitive.
- Empty namespace components are not allowed.

### Table Operations

The Iceberg Catalog supports all standard Iceberg table operations:

- Create tables with schemas and partitioning.
- Update table schemas and partitioning.
- Commit changes to tables.
- Delete tables.
- List tables in namespaces.

### Version Control Features

The Iceberg Catalog integrates with lakeFS's version control system, treating each table change as a commit. 
This provides a complete history of table modifications and enables branching and merging workflows.

#### Catalog Changes as Commits

Each modification to a table (schema changes, data updates, etc.) creates a new commit in lakeFS. 
Creating or deleting a namespace or a table results in a lakeFS commit on the relevant branch, as well as table data updates ("Iceberg table commit").

#### Branching

Create a new branch to work on table changes:

```python
# Create a lakeFS branch using lakeFS Python SDK
branch = lakefs.repository('repo').branch('new_branch').create(source_reference='main')

# The table is now accessible in the new branch
new_table = catalog.load_table(f'repo.{branch.id}.inventory.books')
```

#### Merging

Merge changes between branches:

```python
# Merge the branch using lakeFS Python SDK
branch.merge_into('main')

# Changes are now visible in main
main_table = catalog.load_table('repo.main.inventory.books')
```

{: .note}
Currently, lakeFS handles table changes as file operations during merges. 
This means that when merging branches with table changes, lakeFS treats the table metadata files as regular files. 
No special merge logic is applied to handle conflicting table changes, and if there are conflicting changes to the same table in different branches, 
the merge will fail with a conflict that needs to be resolved manually.

### Authentication

lakeFS provides an OAuth2 token endpoint at `/catalog/iceberg/v1/oauth/tokens` that clients need to configure. 
To authenticate, clients must provide their lakeFS access key and secret in the format `access_key:secret` as the credential.

The authorization requirements are managed at the lakeFS level, meaning:

- Users need appropriate lakeFS permissions to access repositories and branches
- Table operations require lakeFS permissions on the underlying objects
- The same lakeFS RBAC policies apply to Iceberg catalog operations

## Limitations

### Current Limitations

The following features are *not yet supported or implemented*:

1. **Table Maintenance**:
   - See [Table Maintenance](#table-maintenance) section for details

2. **Catalog Sync**:
   - Push/pull operations with other catalogs

3. **Advanced Features**:
   - Views (all view operations are unsupported)
   - Transactional changes (`stage-create`)
   - Multi-table transactions
   - Server-side query planning
   - Table renaming
   - Updating table's location (using Commit)
   - Table statistics (set-statistics and remove-statistics operations)

4. **Advanced Merging**:
   - Merging tables with conflicting changes
   - Specialized merge strategies for different table operations

5. **Table Registration**:
   - Registering existing Iceberg tables from other catalogs
   - Importing tables from external sources

In addition, currently only [Iceberg `v2` table format](https://iceberg.apache.org/spec) is supported.

### Table Maintenance

The following table maintenance operations are *not* supported in the current version:

- [Drop table with purge](https://iceberg.apache.org/docs/latest/spark-ddl/#drop-table-purge)
- [Compact data files](https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files)
- [Rewrite manifests](https://iceberg.apache.org/docs/latest/maintenance/#rewrite-manifests)
- [Expire snapshots](https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots)
- [Remove old metadata files](https://iceberg.apache.org/docs/latest/maintenance/#remove-old-metadata-files)
- [Delete orphan files](https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files)

{: .warning}
> To prevent data loss, **clients should disable their own cleanup operations** by:
> - Disabling orphan file deletion.
> - Setting `remove-dangling-deletes` to false when rewriting.
> - Disabling snapshot expiration.
> - Setting a very high value for `min-snapshots-to-keep` parameter.

#### Storage Compatibility

lakeFS Tables were tested to work with Amazon S3 and Google Cloud Storage.
Other storage backends, such as Azure or Local storage are currently not supported, but will be in future releases.

## Future Releases

The following features are planned for future releases:

1. **Catalog Sync**:
   - Support for pushing/pulling tables to/from other catalogs.
   - Integration with AWS Glue and other Iceberg-compatible catalogs.

2. **Table Import**:
   - Support for importing existing Iceberg tables from other catalogs.
   - Bulk import capabilities for large-scale migrations.

3. **Advanced Features**:
   - Views API support.
   - Table transactions.

4. **Azure Storage Support**

## Related Resources

- [Iceberg REST Catalog API Specification](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml)
- [Iceberg Official Documentation](https://iceberg.apache.org/docs/latest/)
- [lakeFS Enterprise Features]({% link enterprise/index.md %})

