# Unity catalog exporter

## Introduction

Currently, due to the limitations of Databricks Unity catalog, which supports only cloud provider direct storage
endpoints and authentication, it's not feasible to configure it to work directly with lakeFS.  
We wish to overcome this limitation to enable Unity catalog-backed services to read Delta Lake tables, which is the
default table format used by Databricks, from lakeFS.

---

## Proposed Solution

Following the [catalog exports issue](https://github.com/treeverse/lakeFS/issues/6461), the Unity catalog exporter will 
utilize the [Delta Lake catalog exporter](./delta-catalog-exporter.md) to export an existing Delta Lake table to 
`${storageNamespace}/_lakefs/exported/${ref}/${commitId}/${tableName}`. Following this, it will create an external table
in an existing `catalog.schema` within the Unity catalog, using the Databricks API, the provided 
`_lakefs_tables/<table>.yaml` definitions by the user, and specifying the location where the Delta Log was exported to.

### Flow

1. Execute the Delta Lake catalog exporter procedure and retrieve the path to the exported data.
2. Utilizing the table names configured for this hook, such as `['my-table', 'my-other-table']`, establish or replace external
tables within the Unity catalog (which is provided in the hook's configuration) and schema (which will be the branch). Ensure that you use
the field names and data types as specified in the `_lakefs_tables/my-table.yaml` and `_lakefs_tables/my-other-table.yaml` files.

Once the above hook's run completed successfully, the tables could be read form the Databricks Unity catalog backed service.

## Misc.

- Authentication with Databricks will require a [service principal](https://docs.databricks.com/en/dev-tools/service-principals.html)
and an associated [token](https://docs.databricks.com/en/dev-tools/service-principals.html#step-4-generate-a-databricks-personal-access-token-for-the-databricks-service-principal) to be provided to the hook's
configurations. The Service Principal should have `Service principal: Manager`
permission over itself (Workspace: Admin console -> Service principals -> `<service principal>` -> Permissions -> Grant access (`<service principal>`:
Service principal: Manager), `Workspace access` and `Databricks SQL access` checked (Admin console -> Service principals -> `<service principal>` -> Configurations),
a SQL warehouse that will run `CREATE EXTERNAL TABLE` queries, that allow the service principal to use it (SQL Warehouses ->
`<SQL warehouse>` -> Permissions -> `<service principal>`: Can use), a Catalog that has granted a permission for the service principal
to use it and to create and use a schema within it (Catalog -> `<catalog name>` -> Permissions -> Grant -> `<service principal>`: `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA`),
a Schema with `CREATE TABLE` permission configured for the service principal (Catalog -> `<catalog name>` -> `<schema name>` -> Permissions
-> Grant -> `<service principal>`: `CREATE TABLE`)
and an **External Location** (Catalog -> External Data -> External Locations -> Create location) to the table's bucket (lakeFS's bucket) with `CREATE EXTERNAL TABLE` permission for the service principal,
- The users will supply an existing catalog under which the schema and table will be created using the [Databricks Go SDK](https://docs.databricks.com/en/dev-tools/sdk-go.html).
