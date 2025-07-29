---
title: Apache Iceberg
description: How to integrate lakeFS with Apache Iceberg
status: enterprise
---

# Using lakeFS with Apache Iceberg

## lakeFS Iceberg REST Catalog

!!! info
    Available in **lakeFS Enterprise**

!!! tip
    lakeFS Iceberg REST Catalog is currently in private preview for [lakeFS Enterprise](../enterprise/index.md) customers.
    [Contact us](https://lakefs.io/lp/iceberg-rest-catalog/) to get started!

### What is lakeFS Iceberg REST Catalog?

lakeFS Iceberg REST Catalog allow you to use lakeFS as a [spec-compliant](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) Apache [Iceberg REST catalog](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml), 
allowing Iceberg clients to manage and access tables using a standard REST API. 


![lakeFS Iceberg REST Catalog](../assets/img/lakefs_iceberg_rest_catalog.png)


Using lakeFS Iceberg REST Catalog, you can use lakeFS a drop-in replacement for other Iceberg catalogs like AWS Glue, Nessie, Hive Metastore - or the lakeFS HadoopCatalog (see [below](#deprecated-iceberg-hadoopcatalog))

With lakeFS Iceberg REST Catalog, you can:

- Manage Iceberg tables with full version control capabilities.
- Use standard Iceberg clients and tools without modification.
- Leverage lakeFS's branching and merging features for managing table's lifecycle.
- Maintain data consistency across different environments.

### Use Cases

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
    - Collaborate using [pull requests](../howto/pull-requests.md) on changes to data and schema
4. **Manage and Govern Access to data**:
    - Use the detailed built-in commit log capturing who, what and how data is changed
    - Manage access using fine grained access control to users and groups using RBAC policies
    - Rollback changes atomically and safely to reduce time-to-recover and increase system stability

### Configuration

The Iceberg REST catalog API is exposed at `/iceberg/api` in your lakeFS server. 

To use it:

1. Enable the feature ([contact us](https://lakefs.io/lp/iceberg-rest-catalog/) for details).
2. Configure your Iceberg clients to use the lakeFS REST catalog endpoint.
3. Use your lakeFS access key and secret for authentication.

#### Catalog Initialization Example (using [`pyiceberg`](https://py.iceberg.apache.org/))

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(name = "my_catalog", **{
    'prefix': 'lakefs',
    'uri': f'{lakefs_endpoint}/iceberg/api',
    'oauth2-server-uri': f'{lakefs_endpoint}/iceberg/api/v1/oauth/tokens',
    'credential': f'{lakefs_client_key}:{lakefs_client_secret}',
})
```

#### Example Client code

=== "Python (PyIceberg)"
    ```python
    import lakefs
    from pyiceberg.catalog import load_catalog

    # Initialize the catalog
    catalog = RestCatalog(name = "my_catalog", **{
        'prefix': 'lakefs',
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
=== "Trino"
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
=== "Spark"
    ```scala
    // Configure Spark to use the lakeFS REST catalog
    spark.sql("USE my_repo.main.inventory")

    // List available tables
    spark.sql("SHOW TABLES").show()

    // Query data with branch isolation
    spark.sql("SELECT * FROM books").show()

    // Switch to a feature branch
    spark.sql("USE my_repo.new_branch.inventory")
    spark.sql("SELECT * FROM books").show()
    ```

### Namespaces and Tables

#### Namespace Operations

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


#### Relative Namespace support

Some Apache Iceberg clients do not support nested namespaces.

To support those, the lakeFS REST Catalog allows specifying relative namespaces:
passing a partial namespace as part of the catalog URL endpoint (commonly, `<repository>.<branch>`). 

By doing so, all namespaces passed by the user will be relative to the namespaces passed in the URL.

???+ example "Example: DuckDB"
    DuckDB allows limited nesting in the form of `<database>.<schema>`. When using the [Iceberg REST Catalog integration](https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs.html){ target="_blank"}, the database is replaced by the name given to the catalog.
    We can use relative namespaces to allow scoping the catalog connecting to a specific repository and branch:
    ```sql
    LOAD iceberg;
    LOAD httpfs;

    CREATE SECRET lakefs_credentials (
        TYPE ICEBERG,
        CLIENT_ID '...',
        CLIENT_SECRET '...',
        OAUTH2_SERVER_URI 'https://lakefs.example.com/iceberg/api/v1/oauth/tokens'
    );
    
    ATTACH 'lakefs' AS main_branch (
        TYPE iceberg,
        SECRET lakefs_credentials,
        -- notice the "/relative_to/.../" part:
        ENDPOINT 'https://lakefs.example.com/iceberg/relative_to/my-repo.main/api'
    );

    USE main_branch.inventory;
    SELECT * FROM books;
    ```

    See the [DuckDB documentation](https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs.html){ target="_blank" } for a full reference on how to setup an Iceberg catalog integration.

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

#### Branching and Merging

Create a new branch to work on table changes:

```python
# Create a lakeFS branch using lakeFS Python SDK
branch = lakefs.repository('repo').branch('new_branch').create(source_reference='main')

# The table is now accessible in the new branch
new_table = catalog.load_table(f'repo.{branch.id}.inventory.books')
```

Merge changes between branches:

```python
# Merge the branch using lakeFS Python SDK
branch.merge_into('main')

# Changes are now visible in main
main_table = catalog.load_table('repo.main.inventory.books')
```

!!! info
    Currently, lakeFS handles table changes as file operations during merges.

    This means that when merging branches with table changes, lakeFS treats the table metadata files as regular files.

    No special merge logic is applied to handle conflicting table changes, and if there are conflicting changes to the same table in different branches, 
    the merge will fail with a conflict that needs to be resolved manually.

### Authentication

lakeFS provides an OAuth2 token endpoint at `/catalog/iceberg/v1/oauth/tokens` that clients need to configure. 
To authenticate, clients must provide their lakeFS access key and secret in the format `access_key:secret` as the credential.

### Authorization

The authorization requirements are managed at the lakeFS level, meaning:

- Users need appropriate lakeFS permissions to access repositories and branches
- Table operations require lakeFS permissions on the underlying objects
- The same lakeFS RBAC policies apply to Iceberg catalog operations

### Limitations

1. **Table Maintenance**:
    - See [Table Maintenance](#table-maintenance) section for details
2. **Advanced Features**:
    - Views (all view operations are unsupported)
    - Transactional DML (`stage-create`)
    - Server-side query planning
    - Table renaming
    - Updating table's location (using Commit)
    - Table statistics (`set-statistics` and `remove-statistics` operations are currently a no-op)
3. lakeFS Iceberg REST Catalog is currently tested to work with Amazon S3 and Google Cloud Storage. Other storage backends, such as Azure or Local storage are currently not supported, but will be in future releases.
4. Currently only [Iceberg `v2` table format](https://iceberg.apache.org/spec) is supported

### Table Maintenance

The following table maintenance operations are not supported in the current version:

- [Drop table with purge](https://iceberg.apache.org/docs/latest/spark-ddl/#drop-table-purge)
- [Compact data files](https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files)
- [Rewrite manifests](https://iceberg.apache.org/docs/latest/maintenance/#rewrite-manifests)
- [Expire snapshots](https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots)
- [Remove old metadata files](https://iceberg.apache.org/docs/latest/maintenance/#remove-old-metadata-files)
- [Delete orphan files](https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files)


!!! danger
    **To prevent data loss, clients should disable their own cleanup operations by:**

    - Disabling orphan file deletion.
    - Setting `remove-dangling-deletes` to false when rewriting.
    - Disabling snapshot expiration.
    - Setting a very high value for `min-snapshots-to-keep` parameter.


### Roadmap

The following features are planned for future releases:

1. **Catalog Sync**:
    - Support for pushing/pulling tables to/from other catalogs
    - Integration with AWS Glue and other Iceberg-compatible catalogs
1. **Table Import**:
    - Support for importing existing Iceberg tables from other catalogs
    - Bulk import capabilities for large-scale migrations
1. **Azure Storage Support**
1. **Advanced Features**:
    - Views API support
    - Table transactions
1. **Advanced versioning capabilities**
    - merge non-conflicting table updates

### How it works

Under the hood, the lakeFS Iceberg REST Catalog keeps track of each table's metadata file. This is typically referred to as the [table pointer](https://iceberg.apache.org/spec/#overview){ target="_blank" }.

This pointer is stored inside the repository's [storage namespace](../understand/glossary.md#storage-namespace "A storage namespace is a path on the underlying storage that defines where a lakeFS repository stores both metadata and data"). 

When a request is made, the catalog would examine the table's fully qualified namespace: `<repository>.<reference>.<namespace>.<table_name>` to read that special pointer file from the given reference specified,
and returns the underlying object store location of the metadata file to the client. When a table is created or updated, lakeFS would make sure to generate a new metadata file inside the storage namespace, and register
that metadata file as the current pointer for the requested branch.

This approach leverages Iceberg's existing metadata and the immutability of its snapshots: a commit in lakeFS captures a metadata file, which in turn captures manifest lists, manifest files and all related data files.

Besides simply avoiding "double booking" where both Iceberg and lakeFS would need to keep track of which files belong to which version, it also greatly improves the scalability and compatibility of the catalog with the existing Iceberg tool ecosystem.

#### Example: Reading an Iceberg Table

Here's a simplified example of what reading from an Iceberg table would look like:

```mermaid
sequenceDiagram
    Actor Iceberg Client
    participant lakeFS Catalog API
    participant lakeFS
    participant Object Store

    Iceberg Client->>lakeFS Catalog API: get table metadata("repo.branch.table")
    lakeFS Catalog API->>lakeFS: get('repo', 'branch', 'table')
    lakeFS->>lakeFS Catalog API: physical_address
    lakeFS Catalog API->>Iceberg Client: object location ("s3://.../metadata.json")
    Iceberg Client->>Object Store: GetObject
    Object Store->>Iceberg Client: table data
```

#### Example: Writing an Iceberg Table

Here's a simplified example of what writing to an Iceberg table would look like:

```mermaid
sequenceDiagram
    Actor Iceberg Client
    participant lakeFS Catalog API
    participant lakeFS
    participant Object Store
    Iceberg Client->>Object Store: table data
    Iceberg Client->>lakeFS Catalog API: commit
    lakeFS Catalog API->>lakeFS: create branch("branch-tx-uuid")
    lakeFS Catalog API->>lakeFS: put('new table pointer')
    lakeFS->>Object Store: PutObject("metdata.json")
    lakeFS Catalog API->>lakeFS: merge('branch-tx-uuid', 'branch)
    lakeFS Catalog API->>Iceberg Client: done
```

### Related Resources

!!! question "Further Reading"
    - [Iceberg REST Catalog API Specification](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml)
    - [Iceberg Official Documentation](https://iceberg.apache.org/docs/latest/)
    - [lakeFS Enterprise Features](../enterprise/index.md)

---

## Deprecated: Iceberg HadoopCatalog  

!!! warning
    `HadoopCatalog` and other filesystem-based catalogs are currently not recommended by the Apache Iceberg community and come with several limitations around concurrency and tooling.

    As such, the `HadoopCatalog` described in this section is now deprecated and will not receive further updates

<h3>Setup</h3>

=== "Maven"
    Use the following Maven dependency to install the lakeFS custom catalog:

    ```xml
    <dependency>
    <groupId>io.lakefs</groupId>
    <artifactId>lakefs-iceberg</artifactId>
    <version>0.1.4</version>
    </dependency>
    ```

=== "PySpark"
    Include the `lakefs-iceberg` jar in your package list along with Iceberg. For example: 

    ```python
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,io.lakefs:lakefs-iceberg:0.1.4")
    ```  

<h3>Configuration</h3>


=== "PySpark"
    Set up the Spark SQL catalog: 
    ```python
    .config("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog") \
    .config("spark.sql.catalog.lakefs.warehouse", f"lakefs://{repo_name}") \ 
    .config("spark.sql.catalog.lakefs.cache-enabled", "false")
    ```

    Configure the S3A Hadoop FileSystem with your lakeFS connection details.
    Note that these are your lakeFS endpoint and credentials, not your S3 ones.
        
    ```python
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://example-org.us-east-1.lakefscloud.io") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAIO5FODNN7EXAMPLE") \
    .config("spark.hadoop.fs.s3a.secret.key", "wJalrXUtnFEMI/K3MDENG/bPxRfiCYEXAMPLEKEY") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    ```
=== "Spark Shell"

    ```shell
    spark-shell --conf spark.sql.catalog.lakefs="org.apache.iceberg.spark.SparkCatalog" \
        --conf spark.sql.catalog.lakefs.catalog-impl="io.lakefs.iceberg.LakeFSCatalog" \
        --conf spark.sql.catalog.lakefs.warehouse="lakefs://example-repo" \
        --conf spark.sql.catalog.lakefs.cache-enabled="false" \
        --conf spark.hadoop.fs.s3.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
        --conf spark.hadoop.fs.s3a.endpoint="https://example-org.us-east-1.lakefscloud.io" \
        --conf spark.hadoop.fs.s3a.access.key="AKIAIO5FODNN7EXAMPLE" \
        --conf spark.hadoop.fs.s3a.secret.key="wJalrXUtnFEMI/K3MDENG/bPxRfiCYEXAMPLEKEY" \
        --conf spark.hadoop.fs.s3a.path.style.access="true"
    ```

<h3>Using Iceberg tables with HadoopCatalog</h3>

<h4>Create a table</h4>

To create a table on your main branch, use the following syntax:

```sql
CREATE TABLE lakefs.main.db1.table1 (id int, data string);
```

<h4>Insert data into the table</h4>
    
```sql
INSERT INTO lakefs.main.db1.table1 VALUES (1, 'data1');
INSERT INTO lakefs.main.db1.table1 VALUES (2, 'data2');
```

<h4>Create a branch</h4>

We can now commit the creation of the table to the main branch:

```shell
lakectl commit lakefs://example-repo/main -m "my first iceberg commit"
```

Then, create a branch:

```shell
lakectl branch create lakefs://example-repo/dev -s lakefs://example-repo/main
```

<h4>Make changes on the branch</h4>

We can now make changes on the branch:

```sql
INSERT INTO lakefs.dev.db1.table1 VALUES (3, 'data3');
```

<h4>Query the table</h4>

If we query the table on the branch, we will see the data we inserted:

```sql
SELECT * FROM lakefs.dev.db1.table1;
```

Results in:


```text
+----+------+
| id | data |
+----+------+
| 1  | data1|
| 2  | data2|
| 3  | data3|
+----+------+
```

However, if we query the table on the main branch, we will not see the new changes:

```sql
SELECT * FROM lakefs.main.db1.table1;
```

Results in:

```text
+----+------+
| id | data |
+----+------+
| 1  | data1|
| 2  | data2|
+----+------+
```

<h3>Migrating an existing Iceberg table to the Hadoop Catalog</h3>

This is done through an incremental copy from the original table into lakeFS. 

1. Create a new lakeFS repository `lakectl repo create lakefs://example-repo <base storage path>`
2. Initiate a spark session that can interact with the source iceberg table and the target lakeFS catalog. 
    Here's an example of Hadoop and S3 session and lakeFS catalog with [per-bucket config](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/bk_cloud-data-access/content/s3-per-bucket-configs.html): 

    ```java
    SparkConf conf = new SparkConf();
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true");

    // set hadoop on S3 config (source tables we want to copy) for spark
    conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog");
    conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop");
    conf.set("spark.sql.catalog.hadoop_prod.warehouse", "s3a://my-bucket/warehouse/hadoop/");
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    conf.set("spark.hadoop.fs.s3a.bucket.my-bucket.access.key", "<AWS_ACCESS_KEY>");
    conf.set("spark.hadoop.fs.s3a.bucket.my-bucket.secret.key", "<AWS_SECRET_KEY>");

    // set lakeFS config (target catalog and repository)
    conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog");
    conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog");
    conf.set("spark.sql.catalog.lakefs.warehouse", "lakefs://example-repo");
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    conf.set("spark.hadoop.fs.s3a.bucket.example-repo.access.key", "<LAKEFS_ACCESS_KEY>");
    conf.set("spark.hadoop.fs.s3a.bucket.example-repo.secret.key", "<LAKEFS_SECRET_KEY>");
    conf.set("spark.hadoop.fs.s3a.bucket.example-repo.endpoint"  , "<LAKEFS_ENDPOINT>");
    ```
3. Create Schema in lakeFS and copy the data

    Example of copy with spark-sql: 

    ```sql
    -- Create Iceberg Schema in lakeFS
    CREATE SCHEMA IF NOT EXISTS <lakefs-catalog>.<branch>.<db>
    -- Create new iceberg table in lakeFS from the source table (pre-lakeFS)
    CREATE TABLE IF NOT EXISTS <lakefs-catalog>.<branch>.<db> USING iceberg AS SELECT * FROM <iceberg-original-table>
    ```
