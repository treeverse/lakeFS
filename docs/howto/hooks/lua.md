---
title: Lua Hooks
parent: Actions and Hooks
grand_parent: How-To
description: Lua Hooks reference 
redirect_from:
   - /hooks/lua.html
---


# Lua Hooks

lakeFS supports running hooks without relying on external components using an [embedded Lua VM](https://github.com/Shopify/go-lua)

Using Lua hooks, it is possible to pass a Lua script to be executed directly by the lakeFS server when an action occurs.

The Lua runtime embedded in lakeFS is limited for security reasons. It provides a narrow set of APIs and functions that by default do not allow:

1. Accessing any of the running lakeFS server's environment
2. Accessing the local filesystem available the lakeFS process

{% include toc.html %}

## Action File Lua Hook Properties

_See the [Action configuration](./index.md#action-file) for overall configuration schema and details._

| Property      | Description                               | Data Type  | Required                                       | Default Value |
|---------------|-------------------------------------------|------------|------------------------------------------------|---------------|
| `args`        | One or more arguments to pass to the hook | Dictionary | false                                          |               |
| `script`      | An inline Lua script                      | String     | either this or `script_path` must be specified |               |
| `script_path` | The path in lakeFS to a Lua script        | String     | either this or `script` must be specified      |               |


## Example Lua Hooks

For more examples and configuration samples, check out the [examples/hooks/](https://github.com/treeverse/lakeFS/tree/master/examples/hooks) directory in the lakeFS repository. You'll also find step-by-step examples of hooks in action in the [lakeFS samples repository](https://github.com/treeverse/lakeFS-samples/).

### Display information about an event

This example will print out a JSON representation of the event that occurred:

```yaml
name: dump_all
on:
  post-commit:
  post-merge:
  post-create-tag:
  post-create-branch:
hooks:
  - id: dump_event
    type: lua
    properties:
      script: |
        json = require("encoding/json")
        print(json.marshal(action))
```

### Ensure that a commit includes a mandatory metadata field

A more useful example: ensure every commit contains a required metadata field:

```yaml
name: pre commit metadata field check
on:
pre-commit:
    branches:
    - main
    - dev
hooks:
  - id: ensure_commit_metadata
    type: lua
    properties:
      args:
        notebook_url: {"pattern": "my-jupyter.example.com/.*"}
        spark_version:  {}
      script_path: lua_hooks/ensure_metadata_field.lua
```

Lua code at `lakefs://repo/main/lua_hooks/ensure_metadata_field.lua`:

```lua
regexp = require("regexp")
for k, props in pairs(args) do
  current_value = action.commit.metadata[k]
  if current_value == nil then
    error("missing mandatory metadata field: " .. k)
  end
  if props.pattern and not regexp.match(props.pattern, current_value) then
    error("current value for commit metadata field " .. k .. " does not match pattern: " .. props.pattern .. " - got: " .. current_value)
  end
end
```

For more examples and configuration samples, check out the [examples/hooks/](https://github.com/treeverse/lakeFS/tree/master/examples/hooks) directory in the lakeFS repository.

## Lua Library reference

The Lua runtime embedded in lakeFS is limited for security reasons. The provided APIs are shown below.

### `array(table)`

Helper function to mark a table object as an array for the runtime by setting `_is_array: true` metatable field.

### `aws`

### `aws/s3_client`
S3 client library.

```lua
local aws = require("aws")
-- pass valid AWS credentials
local client = aws.s3_client("ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "REGION")
```

### `aws/s3_client.get_object(bucket, key)`

Returns the body (as a Lua string) of the requested object and a boolean value that is true if the requested object exists

### `aws/s3_client.put_object(bucket, key, value)`

Sets the object at the given bucket and key to the value of the supplied value string

### `aws/s3_client.delete_object(bucket [, key])`

Deletes the object at the given key

### `aws/s3_client.list_objects(bucket [, prefix, continuation_token, delimiter])`

Returns a table of results containing the following structure:

* `is_truncated`: (boolean) whether there are more results to paginate through using the continuation token
* `next_continuation_token`: (string) to pass in the next request to get the next page of results
* `results` (table of tables) information about the objects (and prefixes if a delimiter is used)

a result could in one of the following structures

```lua
{
   ["key"] = "a/common/prefix/",
   ["type"] = "prefix"
}
```

or:

```lua
{
   ["key"] = "path/to/object",
   ["type"] = "object",
   ["etag"] = "etagString",
   ["size"] = 1024,
   ["last_modified"] = "2023-12-31T23:10:00Z"
}
```

### `aws/s3_client.delete_recursive(bucket, prefix)`

Deletes all objects under the given prefix

### `aws/glue`

Glue client library.

```lua
local aws = require("aws")
-- pass valid AWS credentials
local glue = aws.glue_client("ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "REGION")
```

### `aws/glue.get_table(database, table [, catalog_id)`

Describe a table from the Glue catalog.

Example:

```lua
local table, exists = glue.get_table(db, table_name)
if exists then
  print(json.marshal(table))
```

### `aws/glue.create_table(database, table_input, [, catalog_id])`

Create a new table in Glue Catalog.
The `table_input` argument is a JSON that is passed "as is" to AWS and is parallel to the AWS SDK [TableInput](https://docs.aws.amazon.com/glue/latest/webapi/API_CreateTable.html#API_CreateTable_RequestSyntax)

Example: 

```lua
local json = require("encoding/json")
local input = {
    Name = "my-table",
    PartitionKeys = array(partitions),
    -- etc...
}
local json_input = json.marshal(input)
glue.create_table("my-db", table_input)
```

### `aws/glue.update_table(database, table_input, [, catalog_id, version_id, skip_archive])`

Update an existing Table in Glue Catalog.
The `table_input` is the same as the argument in `glue.create_table` function.

### `aws/glue.delete_table(database, table_input, [, catalog_id])`

Delete an existing Table in Glue Catalog.

### `azure`

### `azure/blob_client`
Azure blob client library.

```lua
local azure = require("azure")
-- pass valid Azure credentials
local client = azure.blob_client("AZURE_STORAGE_ACCOUNT", "AZURE_ACCESS_KEY")
```

### `azure/blob_client.get_object(path_uri)`

Returns the body (as a Lua string) of the requested object and a boolean value that is true if the requested object exists  
`path_uri` - A valid Azure blob storage uri in the form of `https://myaccount.blob.core.windows.net/mycontainer/myblob`

### `azure/blob_client.put_object(path_uri, value)`

Sets the object at the given bucket and key to the value of the supplied value string  
`path_uri` - A valid Azure blob storage uri in the form of `https://myaccount.blob.core.windows.net/mycontainer/myblob`

### `azure/blob_client.delete_object(path_uri)`

Deletes the object at the given key  
`path_uri` - A valid Azure blob storage uri in the form of `https://myaccount.blob.core.windows.net/mycontainer/myblob`

### `azure/abfss_transform_path(path)`

Transform an HTTPS Azure URL to a ABFSS scheme. Used by the delta_exporter function to support Azure Unity catalog use cases    
`path` - A valid Azure blob storage URL in the form of `https://myaccount.blob.core.windows.net/mycontainer/myblob`

### `crypto`

### `crypto/aes/encryptCBC(key, plaintext)`

Returns a ciphertext for the aes encrypted text

### `crypto/aes/decryptCBC(key, ciphertext)`

Returns the decrypted (plaintext) string for the encrypted ciphertext

### `crypto/hmac/sign_sha256(message, key)`

Returns a SHA256 hmac signature for the given message with the supplied key (using the SHA256 hashing algorithm)

### `crypto/hmac/sign_sha1(message, key)`

Returns a SHA1 hmac signature for the given message with the supplied key (using the SHA1 hashing algorithm)

### `crypto/md5/digest(data)`

Returns the MD5 digest (string) of the given data

### `crypto/sha256/digest(data)`

Returns the SHA256 digest (string) of the given data

### `databricks/client(databricks_host, databricks_service_principal_token)`

Returns a table representing a Databricks client with the `register_external_table` and `create_or_get_schema` methods.

### `databricks/client.create_schema(schema_name, catalog_name, get_if_exists)`

Creates a schema, or retrieves it if exists, in the configured Databricks host's Unity catalog.
If a schema doesn't exist, a new schema with the given `schema_name` will be created under the given `catalog_name`.
Returns the created/fetched schema name.

Parameters:

- `schema_name(string)`: The required schema name
- `catalog_name(string)`: The catalog name under which the schema will be created (or from which it will be fetched)
- `get_if_exists(boolean)`: In case of failure due to an existing schema with the given `schema_name` in the given
`catalog_name`, return the schema.

Example:

```lua
local databricks = require("databricks")
local client = databricks.client("https://my-host.cloud.databricks.com", "my-service-principal-token")
local schema_name = client.create_schema("main", "mycatalog", true)
```

### `databricks/client.register_external_table(table_name, physical_path, warehouse_id, catalog_name, schema_name, metadata)`

Registers an external table under the provided warehouse ID, catalog name, and schema name.
In order for this method call to succeed, an external location should be configured in the catalog, with the 
`physical_path`'s root storage URI (for example: `s3://mybucket`).
Returns the table's creation status.

Parameters:

- `table_name(string)`: Table name.
- `physical_path(string)`: A location to which the external table will refer, e.g. `s3://mybucket/the/path/to/mytable`.
- `warehouse_id(string)`: The SQL warehouse ID used in Databricks to run the `CREATE TABLE` query (fetched from the SQL warehouse
`Connection Details`, or by running `databricks warehouses get`, choosing your SQL warehouse and fetching its ID).
- `catalog_name(string)`: The name of the catalog under which a schema will be created (or fetched from).
- `schema_name(string)`: The name of the schema under which the table will be created.
- `metadata(table)`: A table of metadata to be added to the table's registration. The metadata table should be of the form:
  `{key1 = "value1", key2 = "value2", ...}`.

Example:

```lua
local databricks = require("databricks")
local client = databricks.client("https://my-host.cloud.databricks.com", "my-service-principal-token")
local status = client.register_external_table("mytable", "s3://mybucket/the/path/to/mytable", "examwarehouseple", "my-catalog-name", "myschema")
```

- For the Databricks permissions needed to run this method, check out the [Unity Catalog Exporter]({% link integrations/unity-catalog.md %}) docs.

### `encoding/base64/encode(data)`

Encodes the given data to a base64 string

### `encoding/base64/decode(data)`

Decodes the given base64 encoded data and return it as a string

### `encoding/base64/url_encode(data)`

Encodes the given data to an unpadded alternate base64 encoding defined in RFC 4648.

### `encoding/base64/url_decode(data)`

Decodes the given unpadded alternate base64 encoding defined in RFC 4648 and return it as a string

### `encoding/hex/encode(value)`

Encode the given value string to hexadecimal values (string)

### `encoding/hex/decode(value)`

Decode the given hexadecimal string back to the string it represents (UTF-8)

### `encoding/json/marshal(table)`

Encodes the given table into a JSON string

### `encoding/json/unmarshal(string)`

Decodes the given string into the equivalent Lua structure

### `encoding/yaml/marshal(table)`

Encodes the given table into a YAML string

### `encoding/yaml/unmarshal(string)`

Decodes the given YAML encoded string into the equivalent Lua structure

### `encoding/parquet/get_schema(payload)`

Read the payload (string) as the contents of a Parquet file and return its schema in the following table structure:

```lua
{
  { ["name"] = "column_a", ["type"] = "INT32" },
  { ["name"] = "column_b", ["type"] = "BYTE_ARRAY" }
}
```

### `formats`

### `formats/delta_client(key, secret, region)`

Creates a new Delta Lake client used to interact with the lakeFS server.
- `key`: lakeFS access key id
- `secret`: lakeFS secret access key
- `region`: The region in which your lakeFS server is configured at.

### `formats/delta_client.get_table(repository_id, reference_id, prefix)`

Returns a representation of a Delta Lake table under the given repository, reference, and prefix.
The format of the response is two tables: 
1. the first is a table of the format `{number, {string}}` where `number` is a version in the Delta Log, and the mapped `{string}` 
array contains JSON strings of the different Delta Lake log operations listed in the mapped version entry. e.g.:
```lua
{
  0 = {
    "{\"commitInfo\":...}",
    "{\"add\": ...}",
    "{\"remove\": ...}"
  },
  1 = {
    "{\"commitInfo\":...}",
    "{\"add\": ...}",
    "{\"remove\": ...}"
  }
}
```
2. the second is a table of the metadata of the current table snapshot. The metadata table can be used to initialize the Delta Lake table in an external Catalog.  
It consists of the following fields:
    - `id`: The table's ID
    - `name`: The table's name
    - `description`: The table's description
    - `schema_string`: The table's schema string
    - `partition_columns`: The table's partition columns
    - `configuration`: The table's configuration
    - `created_time`: The table's creation time

### `gcloud`

### `gcloud/gs_client(gcs_credentials_json_string)`

Create a new Google Cloud Storage client using a string that contains a valid [`credentials.json`](https://developers.google.com/workspace/guides/create-credentials) file content.

### `gcloud/gs.write_fuse_symlink(source, destination, mount_info)`

Will create a [gcsfuse symlink](https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/docs/semantics.md#symlink-inodes)
from the source (typically a lakeFS physical address for an object) to a given destination.

`mount_info` is a Lua table with `"from"` and `"to"` keys - since symlinks don't work for `gs://...` URIs, they need to point
to the mounted location instead. `from` will be removed from the beginning of `source`, and `destination` will be added instead.

Example:

```lua
source = "gs://bucket/lakefs/data/abc/def"
destination = "gs://bucket/exported/path/to/object"
mount_info = {
    ["from"] = "gs://bucket",
    ["to"] = "/home/user/gcs-mount"
}
gs.write_fuse_symlink(source, destination, mount_info)
-- Symlink: "/home/user/gcs-mount/exported/path/to/object" -> "/home/user/gcs-mount/lakefs/data/abc/def"
```

### `lakefs`

The Lua Hook library allows calling back to the lakeFS API using the identity of the user that triggered the action.
For example, if user A tries to commit and triggers a `pre-commit` hook - any call made inside that hook to the lakeFS
API, will automatically use user A's identity for authorization and auditing purposes.

### `lakefs/create_tag(repository_id, reference_id, tag_id)`

Create a new tag for the given reference

### `lakefs/diff_refs(repository_id, lef_reference_id, right_reference_id [, after, prefix, delimiter, amount])`

Returns an object-wise diff between `left_reference_id` and `right_reference_id`.

### `lakefs/list_objects(repository_id, reference_id [, after, prefix, delimiter, amount])`

List objects in the specified repository and reference (branch, tag, commit ID, etc.).
If delimiter is empty, will default to a recursive listing. Otherwise, common prefixes up to `delimiter` will be shown as a single entry.

### `lakefs/get_object(repository_id, reference_id, path)`

Returns 2 values:

1. The HTTP status code returned by the lakeFS API
1. The content of the specified object as a lua string

### `lakefs/diff_branch(repository_id, branch_id [, after, amount, prefix, delimiter])`

Returns an object-wise diff of uncommitted changes on `branch_id`.

### `lakefs/stat_object(repository_id, ref_id, path)`

Returns a stat object for the given path under the given reference and repository.

### `lakefs/catalogexport/glue_exporter.get_full_table_name(descriptor, action_info)`

Generate glue table name.

Parameters:

- `descriptor(Table)`: Object from (e.g. _lakefs_tables/my_table.yaml).
- `action_info(Table)`: The global action object.

### `lakefs/catalogexport/delta_exporter`

A package used to export Delta Lake tables from lakeFS to an external cloud storage.

### `lakefs/catalogexport/delta_exporter.export_delta_log(action, table_def_names, write_object, delta_client, table_descriptors_path, path_transformer)`

The function used to export Delta Lake tables.
The return value is a table with mapping of table names to external table location (from which it is possible to query the data) and latest Delta table version's metadata.  
The response is of the form: 
`{<table_name> = {path = "s3://mybucket/mypath/mytable", metadata = {id = "table_id", name = "table_name", ...}}}`.

Parameters:

- `action`: The global action object
- `table_def_names`: Delta tables name list (e.g. `{"table1", "table2"}`)
- `write_object`: A writer function with `function(bucket, key, data)` signature, used to write the exported Delta Log (e.g. `aws/s3_client.put_object` or `azure/blob_client.put_object`)
- `delta_client`: A Delta Lake client that implements `get_table: function(repo, ref, prefix)`
- `table_descriptors_path`: The path under which the table descriptors of the provided `table_def_names` reside
- `path_transformer`: (Optional) A function(path) used for transforming the path of the saved delta logs path fields as well as the saved table physical path (used to support Azure Unity catalog use cases)

Delta export example for AWS S3:

```yaml
---
name: delta_exporter
on:
  post-commit: null
hooks:
  - id: delta_export
    type: lua
    properties:
      script: |
        local aws = require("aws")
        local formats = require("formats")
        local delta_exporter = require("lakefs/catalogexport/delta_exporter")
        local json = require("encoding/json")

        local table_descriptors_path = "_lakefs_tables"
        local sc = aws.s3_client(args.aws.access_key_id, args.aws.secret_access_key, args.aws.region)
        local delta_client = formats.delta_client(args.lakefs.access_key_id, args.lakefs.secret_access_key, args.aws.region)
        local delta_table_details = delta_export.export_delta_log(action, args.table_defs, sc.put_object, delta_client, table_descriptors_path)
        
        for t, details in pairs(delta_table_details) do
          print("Delta Lake exported table \"" .. t .. "\"'s location: " .. details["path"] .. "\n")
          print("Delta Lake exported table \"" .. t .. "\"'s metadata:\n")
          for k, v in pairs(details["metadata"]) do
            if type(v) == "table" then
              print("\t" .. k .. " = " .. json.marshal(v) .. "\n")
            else
              print("\t" .. k .. " = " .. v .. "\n")
            end
          end
        end
      args:
        aws:
          access_key_id: <AWS_ACCESS_KEY_ID>
          secret_access_key: <AWS_SECRET_ACCESS_KEY>
          region: us-east-1
        lakefs:
          access_key_id: <LAKEFS_ACCESS_KEY_ID> 
          secret_access_key: <LAKEFS_SECRET_ACCESS_KEY>
        table_defs:
          - mytable
```

For the table descriptor under the `_lakefs_tables/mytable.yaml`:
```yaml
---
name: myTableActualName
type: delta
path: a/path/to/my/delta/table
```

Delta export example for Azure Blob Storage:

```yaml
name: Delta Exporter
on:
  post-commit:
    branches: ["{{ .Branch }}*"]
hooks:
  - id: delta_exporter
    type: lua
    properties:
      script: |
        local azure = require("azure")
        local formats = require("formats")
        local delta_exporter = require("lakefs/catalogexport/delta_exporter")

        local table_descriptors_path = "_lakefs_tables"
        local sc = azure.blob_client(args.azure.storage_account, args.azure.access_key)
        local function write_object(_, key, buf)
          return sc.put_object(key,buf)
        end
        local delta_client = formats.delta_client(args.lakefs.access_key_id, args.lakefs.secret_access_key)
        local delta_table_details = delta_export.export_delta_log(action, args.table_defs, sc.put_object, delta_client, table_descriptors_path)
        
        for t, details in pairs(delta_table_details) do
          print("Delta Lake exported table \"" .. t .. "\"'s location: " .. details["path"] .. "\n")
          print("Delta Lake exported table \"" .. t .. "\"'s metadata:\n")
          for k, v in pairs(details["metadata"]) do
            if type(v) == "table" then
              print("\t" .. k .. " = " .. json.marshal(v) .. "\n")
            else
              print("\t" .. k .. " = " .. v .. "\n")
            end
          end
        end
      args:
        azure:
          storage_account: "{{ .AzureStorageAccount }}"
          access_key: "{{ .AzureAccessKey }}"
        lakefs: # provide credentials of a user that has access to the script and Delta Table
          access_key_id: "{{ .LakeFSAccessKeyID }}"
          secret_access_key: "{{ .LakeFSSecretAccessKey }}"
        table_defs:
          - mytable

```

### `lakefs/catalogexport/table_extractor`

Utility package to parse `_lakefs_tables/` descriptors.

### `lakefs/catalogexport/table_extractor.list_table_descriptor_entries(client, repo_id, commit_id)`

List all YAML files under `_lakefs_tables/*` and return a list of type `[{physical_address, path}]`, ignores hidden files. 
The `client` is `lakefs` client.

### `lakefs/catalogexport/table_extractor.get_table_descriptor(client, repo_id, commit_id, logical_path)`

Read a table descriptor and parse YAML object. Will set `partition_columns` to `{}` if no partitions are defined.
The `client` is `lakefs` client.

### `lakefs/catalogexport/hive.extract_partition_pager(client, repo_id, commit_id, base_path, partition_cols, page_size)`

Hive format partition iterator each result set is a collection of files under the same partition in lakeFS.

Example: 

```lua
local lakefs = require("lakefs")
local pager = hive.extract_partition_pager(lakefs, repo_id, commit_id, prefix, partitions, 10)
for part_key, entries in pager do
    print("partition: " .. part_key)
    for _, entry in ipairs(entries) do
        print("path: " .. entry.path .. " physical: " .. entry.physical_address)
    end
end
```

### `lakefs/catalogexport/symlink_exporter`

Writes metadata for a table using Hive's [SymlinkTextInputFormat](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/io/SymlinkTextInputFormat.html).
Currently only `S3` is supported.

The default export paths per commit:

```
${storageNamespace}
_lakefs/
    exported/
        ${ref}/
            ${commitId}/
                ${tableName}/
                    p1=v1/symlink.txt
                    p1=v2/symlink.txt
                    p1=v3/symlink.txt
                    ...
```

### `lakefs/catalogexport/symlink_exporter.export_s3(s3_client, table_src_path, action_info [, options])`

Export Symlink files that represent a table to S3 location.

Parameters:

- `s3_client`: Configured client.
- `table_src_path(string)`: Path to the table spec YAML file in `_lakefs_tables` (e.g. _lakefs_tables/my_table.yaml).
- `action_info(table)`: The global action object.
- `options(table)`:
  - `debug(boolean)`: Print extra info.
  - `export_base_uri(string)`: Override the prefix in S3 e.g. `s3://other-bucket/path/`.
  - `writer(function(bucket, key, data))`: If passed then will not use s3 client, helpful for debug.

Example:

```lua
local exporter = require("lakefs/catalogexport/symlink_exporter")
local aws = require("aws")
-- args are user inputs from a lakeFS action.
local s3 = aws.s3_client(args.aws.aws_access_key_id, args.aws.aws_secret_access_key, args.aws.aws_region)
exporter.export_s3(s3, args.table_descriptor_path, action, {debug=true})
```

### `lakefs/catalogexport/glue_exporter`

A Package for automating the export process from lakeFS stored tables into Glue catalog.

### `lakefs/catalogexport/glue_exporter.export_glue(glue, db, table_src_path, create_table_input, action_info, options)`

Represent lakeFS table in Glue Catalog. 
This function will create a table in Glue based on configuration. 
It assumes that there is a symlink location that is already created and only configures it by default for the same commit.

Parameters:

- `glue`: AWS glue client
- `db(string)`: glue database name
- `table_src_path(string)`: path to table spec (e.g. _lakefs_tables/my_table.yaml)
- `create_table_input(Table)`: Input equal mapping to [table_input](https://docs.aws.amazon.com/glue/latest/webapi/API_CreateTable.html#API_CreateTable_RequestSyntax) in AWS, the same as we use for `glue.create_table`.
should contain inputs describing the data format (e.g. InputFormat, OutputFormat, SerdeInfo) since the exporter is agnostic to this. 
by default this function will configure table location and schema.
- `action_info(Table)`: the global action object.
- `options(Table)`:
  - `table_name(string)`: Override default glue table name
  - `debug(boolean`
  - `export_base_uri(string)`: Override the default prefix in S3 for symlink location e.g. s3://other-bucket/path/

When creating a glue table, the final table input will consist of the `create_table_input` input parameter and lakeFS computed defaults that will override it:

- `Name` Gable table name `get_full_table_name(descriptor, action_info)`.
- `PartitionKeys` Partition columns usually deduced from `_lakefs_tables/${table_src_path}`.
- `TableType` = "EXTERNAL_TABLE"
- `StorageDescriptor`: Columns usually deduced from `_lakefs_tables/${table_src_path}`.
- `StorageDescriptor.Location` = symlink_location

Example: 

```lua
local aws = require("aws")
local exporter = require("lakefs/catalogexport/glue_exporter")
local glue = aws.glue_client(args.aws_access_key_id, args.aws_secret_access_key, args.aws_region)
-- table_input can be passed as a simple Key-Value object in YAML as an argument from an action, this is inline example:
local table_input = {
  StorageDescriptor: 
    InputFormat: "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
    OutputFormat: "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"
    SerdeInfo:
      SerializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  Parameters: 
    classification: "parquet"
    EXTERNAL: "TRUE"
    "parquet.compression": "SNAPPY"
}
exporter.export_glue(glue, "my-db", "_lakefs_tables/animals.yaml", table_input, action, {debug=true})
```

### `lakefs/catalogexport/glue_exporter.get_full_table_name(descriptor, action_info)`

Generate glue table name.

Parameters:

- `descriptor(Table)`: Object from (e.g. _lakefs_tables/my_table.yaml).
- `action_info(Table)`: The global action object.

### `lakefs/catalogexport/unity_exporter`

A package used to register exported Delta Lake tables to Databricks' Unity catalog.

### `lakefs/catalogexport/unity_exporter.register_tables(action, table_descriptors_path, delta_table_details, databricks_client, warehouse_id)`

The function used to register exported Delta Lake tables in Databricks' Unity Catalog.
The registration will use the following paths to register the table:
`<catalog>.<branch name>.<table_name>` where the branch name will be used as the schema name.
The return value is a table with mapping of table names to registration request status.

**Note: (Azure users)** Databricks catalog external locations is supported only for ADLS Gen2 storage accounts.  
When exporting Delta tables using the `lakefs/catalogexport/delta_exporter.export_delta_log` function, the `path_transformer` must be  
used to convert the paths scheme to `abfss`. The built-in `azure` lua library provides this functionality with `transformPathToAbfss`.

Parameters:

- `action(table)`: The global action table
- `table_descriptors_path(string)`: The path under which the table descriptors of the provided `table_paths` reside.
- `delta_table_details(table)`: Table names to physical paths mapping and table metadata (e.g. `{table1 = {path = "s3://mybucket/mytable1", metadata = {id = "table_1_id", name = "table1", ...}}, table2 = {path = "s3://mybucket/mytable2", metadata = {id = "table_2_id", name = "table2", ...}}}`.)
- `databricks_client(table)`: A Databricks client that implements `create_or_get_schema: function(id, catalog_name)` and `register_external_table: function(table_name, physical_path, warehouse_id, catalog_name, schema_name)`
- `warehouse_id(string)`: Databricks warehouse ID.

Example:
The following registers an exported Delta Lake table to Unity Catalog.

```lua
local databricks = require("databricks")
local unity_export = require("lakefs/catalogexport/unity_exporter")

local delta_table_locations = {
  ["table1"] = "s3://mybucket/mytable1",
}
-- Register the exported table in Unity Catalog:
local action_details = {
  repository_id = "my-repo",
  commit_id = "commit_id",
  branch_id = "main",
}
local databricks_client = databricks.client("<DATABRICKS_HOST>", "<DATABRICKS_TOKEN>")
local registration_statuses = unity_export.register_tables(action_details, "_lakefs_tables", delta_table_locations, databricks_client, "<WAREHOUSE_ID>")

for t, status in pairs(registration_statuses) do
  print("Unity catalog registration for table \"" .. t .. "\" completed with status: " .. status .. "\n")
end
```

For the table descriptor under the `_lakefs_tables/delta-table-descriptor.yaml`:
```yaml
---
name: my_table_name
type: delta
path: path/to/delta/table/data
catalog: my-catalog
```

For detailed step-by-step guide on how to use `unity_exporter.register_tables` as a part of a lakeFS action refer to
the [Unity Catalog docs]({% link integrations/unity-catalog.md %}).

### `path/parse(path_string)`

Returns a table for the given path string with the following structure:

```lua
> require("path")
> path.parse("a/b/c.csv")
{
    ["parent"] = "a/b/"
    ["base_name"] = "c.csv"
} 
```

### `path/join(*path_parts)`

Receives a variable number of strings and returns a joined string that represents a path:

```lua
> require("path")
> path.join("path/", "to", "a", "file.data")
path/o/a/file.data
```

### `path/is_hidden(path_string [, seperator, prefix])`

returns a boolean - `true` if the given path string is hidden (meaning it starts with `prefix`) - or if any of its parents start with `prefix`.

```lua
> require("path")
> path.is_hidden("a/b/c") -- false
> path.is_hidden("a/b/_c") -- true
> path.is_hidden("a/_b/c") -- true
> path.is_hidden("a/b/_c/") -- true
```
### `path/default_separator()`

Returns a constant string (`/`)

### `regexp/match(pattern, s)`

Returns true if the string `s` matches `pattern`.
This is a thin wrapper over Go's [regexp.MatchString](https://pkg.go.dev/regexp#MatchString){: target="_blank" }.

### `regexp/quote_meta(s)`

Escapes any meta-characters in string `s` and returns a new string

### `regexp/compile(pattern)`

Returns a regexp match object for the given pattern

### `regexp/compiled_pattern.find_all(s, n)`

Returns a table list of all matches for the pattern, (up to `n` matches, unless `n == -1` in which case all possible matches will be returned)

### `regexp/compiled_pattern.find_all_submatch(s, n)`

Returns a table list of all sub-matches for the pattern, (up to `n` matches, unless `n == -1` in which case all possible matches will be returned).
Submatches are matches of parenthesized subexpressions (also known as capturing groups) within the regular expression,
numbered from left to right in order of opening parenthesis.
Submatch 0 is the match of the entire expression, submatch 1 is the match of the first parenthesized subexpression, and so on

### `regexp/compiled_pattern.find(s)`

Returns a string representing the left-most match for the given pattern in string `s`

### `regexp/compiled_pattern.find_submatch(s)`

find_submatch returns a table of strings holding the text of the leftmost match of the regular expression in `s` and the matches, if any, of its submatches

### `strings/split(s, sep)`

returns a table of strings, the result of splitting `s` with `sep`.

### `strings/trim(s)`

Returns a string with all leading and trailing white space removed, as defined by Unicode

### `strings/replace(s, old, new, n)`

Returns a copy of the string s with the first n non-overlapping instances of `old` replaced by `new`.
If `old` is empty, it matches at the beginning of the string and after each UTF-8 sequence, yielding up to k+1 replacements for a k-rune string.

If n < 0, there is no limit on the number of replacements

### `strings/has_prefix(s, prefix)`

Returns `true` if `s` begins with `prefix`

### `strings/has_suffix(s, suffix)`

Returns `true` if `s` ends with `suffix`

### `strings/contains(s, substr)`

Returns `true` if `substr` is contained anywhere in `s`

### `time/now()`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00).

### `time/format(epoch_nano, layout, zone)`

Returns a string representation of the given epoch_nano timestamp for the given Timezone (e.g. `"UTC"`, `"America/Los_Angeles"`, ...)
The `layout` parameter should follow [Go's time layout format](https://pkg.go.dev/time#pkg-constants){: target="_blank" }.

### `time/format_iso(epoch_nano, zone)`

Returns a string representation of the given `epoch_nano` timestamp for the given Timezone (e.g. `"UTC"`, `"America/Los_Angeles"`, ...)
The returned string will be in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601){: target="_blank" } format.

### `time/sleep(duration_ns)`

Sleep for `duration_ns` nanoseconds

### `time/since(epoch_nano)`

Returns the amount of nanoseconds elapsed since `epoch_nano`

### `time/add(epoch_time, duration_table)`

Returns a new timestamp (in nanoseconds passed since 01/01/1970 00:00:00) for the given `duration`.
The `duration` should be a table with the following structure:

```lua
> require("time")
> time.add(time.now(), {
    ["hour"] = 1,
    ["minute"] = 20,
    ["second"] = 50
})
```
You may omit any of the fields from the table, resulting in a default value of `0` for omitted fields

### `time/parse(layout, value)`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00).
This timestamp will represent date `value` parsed using the `layout` format.

The `layout` parameter should follow [Go's time layout format](https://pkg.go.dev/time#pkg-constants){: target="_blank" }

### `time/parse_iso(value)`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00 for `value`.
The `value` string should be in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601){: target="_blank" } format

### `uuid/new()`

Returns a new 128-bit [RFC 4122 UUID](https://www.rfc-editor.org/rfc/rfc4122){: target="_blank" } in string representation.

### `net/url`

Provides a `parse` function parse a URL string into parts, returns a table with the URL's host, path, scheme, query and fragment.

```lua
> local url = require("net/url")
> url.parse("https://example.com/path?p1=a#section")
{
    ["host"] = "example.com"
    ["path"] = "/path"
    ["scheme"] = "https"
    ["query"] = "p1=a"
    ["fragment"] = "section"
}
```


### `net/http` (optional)

Provides a `request` function that performs an HTTP request.
For security reasons, this package is not available by default as it enables http requests to be sent out from the lakeFS instance network. The feature should be enabled under `actions.lua.net_http_enabled` [configuration]({% link reference/configuration.md %}).
Request will time out after 30 seconds.

```lua
http.request(url [, body])
http.request{
  url = string,
  [method = string,]
  [headers = header-table,]
  [body = string,]
}
```

Returns a code (number), body (string), headers (table) and status (string).

 - code - status code number
 - body - string with the response body
 - headers - table with the response request headers (key/value or table of values)
 - status - status code text

The first form of the call will perform GET requests or POST requests if the body parameter is passed.

The second form accepts a table and allows you to customize the request method and headers.


Example of a GET request

```lua
local http = require("net/http")
local code, body = http.request("https://example.com")
if code == 200 then
    print(body)
else
    print("Failed to get example.com - status code: " .. code)
end

```

Example of a POST request

```lua
local http = require("net/http")
local code, body = http.request{
    url="https://httpbin.org/post",
    method="POST",
    body="custname=tester",
    headers={["Content-Type"]="application/x-www-form-urlencoded"},
}
if code == 200 then
    print(body)
else
    print("Failed to post data - status code: " .. code)
end
```
