---
layout: default
title: Hooks
parent: Reference
description: Hooks configuration and API reference
nav_order: 30
has_children: false
redirect_from: ../hooks.html
---
# Hooks
{: .no_toc }

{% include toc.html %}

Like other version control systems, lakeFS allows you to configure `Actions` to trigger when predefined events occur.

Supported Events:

| Event                | Description                                                                    |
|----------------------|--------------------------------------------------------------------------------|
| `pre-commit`         | Runs when the commit occurs, before the commit is finalized                    |
| `post-commit`        | Runs after the commit is finalized                                             |
| `pre-merge`          | Runs on the source branch when the merge occurs, before the merge is finalized |
| `post-merge`         | Runs on the merge result, after the merge is finalized                         |
| `pre-create-branch`  | Runs on the source branch prior to creating a new branch                       |
| `post-create-branch` | Runs on the new branch after the branch was created                            |
| `pre-delete-branch`  | Runs prior to deleting a branch                                                |
| `post-delete-branch` | Runs after the branch was deleted                                              |
| `pre-create-tag`     | Runs prior to creating a new tag                                               |
| `post-create-tag`    | Runs after the tag was created                                                 |
| `pre-delete-tag`     | Runs prior to deleting a tag                                                   |
| `post-delete-tag`    | Runs after the tag was deleted                                                 |

lakeFS Actions are handled per repository and cannot be shared between repositories.
A failure of any Hook under any Action of a `pre-*` event will result in aborting the lakeFS operation that is taking place.
Hook failures under any Action of a `post-*` event will not revert the operation.

Hooks are managed by Action files that are written to a prefix in the lakeFS repository.
This allows configuration-as-code inside lakeFS, where Action files are declarative and written in YAML.

## Example use cases

1. Format Validator:
   A webhook that checks new files to ensure they are of a set of allowed data format.
1. Schema Validator:
   A webhook that reads new Parquet and ORC files to ensure they don't contain a block list of column names (or name prefixes).
   This is useful for avoiding accidental PII exposure.
1. Integration with external systems:
   Post-merge and post-commit hooks could be used to export metadata about the change to another system. A common example is exporting `symlink.txt` files that allow e.g. [AWS Athena](../integrations/athena.md) to read data from lakeFS.
1. Notifying downstream consumers:
   Running a post-merge hook to trigger an Airflow DAG or to send a Webhook to an API, notifying it of the change that happened

For more examples and configuration samples, check out the [examples](https://github.com/treeverse/lakeFS/tree/master/examples/hooks) on the lakeFS repository.

## Terminology

### Action
{: .no_toc }

An **Action** is a list of Hooks with the same trigger configuration, i.e. an event will trigger all Hooks under an Action or none at all.
The Hooks under an Action are ordered and so is their execution. A Hook will only be executed if all the previous Hooks that were triggered with it had passed.

### Hook
{: .no_toc }

A **Hook** is the basic building block of an Action.
The failure of a single Hook will stop the execution of the containing Action and fail the Run.

### Action file
{: .no_toc }

Schema of the Action file:

| Property           | Description                                           | Data Type  | Required | Default Value                                                           |
|--------------------|-------------------------------------------------------|------------|----------|-------------------------------------------------------------------------|
| name               | Identify the Action file                              | String     | false    | If missing, filename is used instead                                    |
| on                 | List of events that will trigger the hooks            | List       | true     |                                                                         |
| on<event>.branches | Glob pattern list of branches that triggers the hooks | List       | false    | **Not applicable to Tag events.** If empty, Action runs on all branches |
| hooks              | List of hooks to be executed                          | List       | true     |                                                                         |
| hook.id            | ID of the hook, must be unique within the `Action`    | String     | true     |                                                                         |
| hook.type          | Type of the hook ([types](#hook-types))               | String     | true     |                                                                         |
| hook.properties    | Hook's specific configuration                         | Dictionary | true     |                                                                         |

Example:

```yaml
name: Good files check
description: set of checks to verify that branch is good
on:
  pre-commit:
  pre-merge:
    branches:
      - main
hooks:
  - id: no_temp
    type: webhook
    description: checking no temporary files found
    properties:
      url: "https://your.domain.io/webhook?notmp=true?t=1za2PbkZK1bd4prMuTDr6BeEQwWYcX2R"
  - id: no_freeze
    type: webhook
    description: check production is not in dev freeze
    properties:
      url: "https://your.domain.io/webhook?nofreeze=true?t=1za2PbkZK1bd4prMuTDr6BeEQwWYcX2R"
```

**Note:** lakeFS will validate action files only when an **Event** has occurred. <br/>
Use `lakectl actions validate <path>` to validate your action files locally.
{: .note }

### Run
{: .no_toc }

A **Run** is an instantiation of the repository's Action files when the triggering event occurs.
For example, if your repository contains a pre-commit hook, every commit would generate a Run for that specific commit.

lakeFS will fetch, parse and filter the repository Action files and start to execute the Hooks under each Action.
All executed Hooks (each with `hook_run_id`) exist in the context of that Run (`run_id`).

---
## Uploading Action files

Action files should be uploaded with the prefix `_lakefs_actions/` to the lakeFS repository.
When an actionable event (see Supported Events above) takes place, lakeFS will read all files with prefix `_lakefs_actions/`
in the repository branch where the action occurred.
A failure to parse an Action file will result with a failing Run.

For example, lakeFS will search and execute all the matching Action files with the prefix `lakefs://example-repo/feature-1/_lakefs_actions/` on:
1. Commit to `feature-1` branch on `example-repo` repository.
1. Merge to `main` branch from `feature-1` branch on `repo1` repository.

## Runs API & CLI

The [lakeFS API](./api.md) and [lakectl](./cli.md#lakectl-actions) expose the results of executions per repository, branch, commit, and specific Action.
The endpoint also allows to download the execution log of any executed Hook under each Run for observability.


### Result Files
{: .no_toc }

The metadata section of lakeFS repository with each Run contains two types of files:
1. `_lakefs/actions/log/<runID>/<hookRunID>.log` - Execution log of the specific Hook run.
1. `_lakefs/actions/log/<runID>/run.manifest` - Manifest with all Hooks execution for the run with their results and additional metadata.

**Note:** Metadata section of a lakeFS repository is where lakeFS keeps its metadata, like commits and metaranges.
Metadata files stored in the metadata section aren't accessible like user stored files.
{: .note }

---
## Hook types

Currently, there are two types of Hooks that are supported by lakeFS: [Webhook](#webhooks) and [Airflow](#airflow-hooks).
Experimental support for [Lua](#lua-hooks-experimental) was introduced in lakeFS 0.86.0.

### Webhooks

A Webhook is a Hook type that sends an HTTP POST request to the configured URL.
Any non 2XX response by the responding endpoint will fail the Hook, cancel the execution of the following Hooks
under the same Action. For `pre-*` hooks, the triggering operation will also be aborted.

**Warning:** You should not use `pre-*` webhooks for long-running tasks, since they block the performed operation.
Moreover, the branch is locked during the execution of `pre-*` hooks, so the webhook server cannot perform any write operations on the branch (like uploading or commits).
{: .note }

#### Action file Webhook properties

| Property     | Description                                            | Data Type                                                                                 | Required | Default Value | Env Vars Support |
|--------------|--------------------------------------------------------|-------------------------------------------------------------------------------------------|----------|---------------|------------------|
| url          | The URL address of the request                         | String                                                                                    | true     |               | no               |
| timeout      | Time to wait for response before failing the hook      | String (golang's [Duration](https://golang.org/pkg/time/#Duration.String) representation) | false    | 1 minute      | no               |
| query_params | List of query params that will be added to the request | Dictionary(String:String or String:List(String)                                           | false    |               | yes              |
| headers      | Headers to add to the request                          | Dictionary(String:String)                                                                 | false    |               | yes              |

**Secrets & Environment Variables**<br/>
lakeFS Actions supports secrets by using environment variables.
The format `{% raw %}{{{% endraw %} ENV.SOME_ENV_VAR {% raw %}}}{% endraw %}` will be replaced with the value of `$SOME_ENV_VAR`
during the execution of the action. If that environment variable doesn't exist in the lakeFS server environment, the action run will fail.
{: .note }

Example:

```yaml
...
hooks:
  - id: prevent_user_columns
    type: webhook
    description: Ensure no user_* columns under public/
    properties:
      url: "http://<host:port>/webhooks/schema"
      timeout: 1m30s
      query_params:
        disallow: ["user_", "private_"]
        prefix: public/
      headers:
        secret_header: "{% raw %}{{{% endraw %} ENV.MY_SECRET {% raw %}}}{% endraw %}"
...
```

#### Request body schema
Upon execution, a webhook will send a request containing a JSON object with the following fields:

| Field               | Description                                                       | Type   |
|---------------------|-------------------------------------------------------------------|--------|
| event_type          | Type of the event that triggered the `Action`                     | string |
| event_time          | Time of the event that triggered the `Action` (RFC3339 formatted) | string |
| action_name         | Containing `Hook` Action's Name                                   | string |
| hook_id             | ID of the `Hook`                                                  | string |
| repository_id       | ID of the Repository                                              | string |
| branch_id[^1]       | ID of the Branch                                                  | string |
| source_ref          | Reference to the source on which the event was triggered          | string |
| commit_message[^2]  | The message for the commit (or merge) that is taking place        | string |
| committer[^2]       | Name of the committer                                             | string |
| commit_metadata[^2] | The metadata for the commit that is taking place                  | string |
| tag_id[^3]          | The ID of the created/deleted tag                                 | string |

[^1]: N\A for Tag events  
[^2]: N\A for Tag and Create/Delete Branch events  
[^3]: Applicable only for Tag events

Example:
```json
{
  "event_type": "pre-merge",
  "event_time": "2021-02-28T14:03:31Z",
  "action_name": "test action",
  "hook_id": "prevent_user_columns",
  "repository_id": "repo1",
  "branch_id": "feature-1",
  "source_ref": "feature-1",
  "commit_message": "merge commit message",
  "committer": "committer",
  "commit_metadata": {
    "key": "value"
  }
}
```

### Airflow Hooks

Airflow Hook triggers a DAG run in an Airflow installation using [Airflow's REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/post_dag_run).
The hook run succeeds if the DAG was triggered, and fails otherwise.

#### Action file Airflow hook properties

| Property      | Description                                                     | Data Type                                                                                 | Example                 | Required | Env Vars Support |
|---------------|-----------------------------------------------------------------|-------------------------------------------------------------------------------------------|-------------------------|----------|------------------|
| url           | The URL of the Airflow instance                                 | String                                                                                    | "http://localhost:8080" | true     | no               |
| dag_id        | The DAG to trigger                                              | String                                                                                    | "example_dag"           | true     | no               |
| username      | The name of the Airflow user performing the request             | String                                                                                    | "admin"                 | true     | no               |
| password      | The password of the Airflow user performing the request         | String                                                                                    | "admin"                 | true     | yes              |
| dag_conf      | DAG run configuration that will be passed as is                 | JSON                                                                                      |                         | false    | no               |
| wait_for_dag  | Wait for DAG run to complete and reflect state (default: false) | Boolean                                                                                   |                         | false    | no               |
| timeout       | Time to wait for the DAG run to complete (default: 1m)          | String (golang's [Duration](https://golang.org/pkg/time/#Duration.String) representation) |                         | false    | no               |

Example:
```yaml
...
hooks:
  - id: trigger_my_dag
    type: airflow
    description: Trigger an example_dag
    properties:
       url: "http://localhost:8000"
       dag_id: "example_dag"
       username: "admin"
       password: "{% raw %}{{{% endraw %} ENV.AIRFLOW_SECRET {% raw %}}}{% endraw %}"
       dag_conf:
          some: "additional_conf"
...
```

#### Hook Record in configuration field

lakeFS will add an entry to the Airflow request configuration property (`conf`) with the event that triggered the action.

The key of the record will be `lakeFS_event` and the value will match the one described [here](#request-body-schema)

### Lua Hooks <span>Experimental</span>{: .label }

As of version 0.86.0, lakeFS supports running hooks without relying on external components using an [embedded Lua VM](https://github.com/Shopify/go-lua)

Using Lua hooks, it is possible to pass a Lua script to be executed directly by the lakeFS server when an action occurs.

The Lua runtime embedded in lakeFS is limited for security reasons. It provides a narrow set of APIs and functions that by default do not allow:

1. Accessing any of the running lakeFS server's environment
2. Accessing the local filesystem available the lakeFS process 


### Lua Library reference

##### `aws/s3.get_object(bucket, key)`

Returns the body (as a Lua string) of the requested object and a boolean value that is true if the requested object exists

##### `aws/s3.put_object(bucket, key, value)`

Sets the object at the given bucket and key to the value of the supplied value string

##### `aws/s3.delete_object(bucket [, key])`

Deletes the object at the given key

##### `aws/s3.list_objects(bucket [, prefix, continuation_token, delimiter])`

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

##### `aws/s3.delete_recursive(bucket, prefix)`

Deletes all objects under the given prefix

##### `crypto/aes/encryptCBC(key, plaintext)`

Returns a ciphertext for the aes encrypted text

##### `crypto/aes/decryptCBC(key, ciphertext)`

Returns the decrypted (plaintext) string for the encrypted ciphertext

##### `crypto/hmac/sign_sha256(message, key)`

Returns a SHA256 hmac signature for the given message with the supplied key (using the SHA256 hashing algorithm)

##### `crypto/hmac/sign_sha1(message, key)`

Returns a SHA1 hmac signature for the given message with the supplied key (using the SHA1 hashing algorithm)

##### `crypto/md5/digest(data)`

Returns the MD5 digest (string) of the given data

##### `crypto/sha256/digest(data)`

Returns the SHA256 digest (string) of the given data

##### `encoding/base64/encode(data)`

Encodes the given data to a base64 string

##### `encoding/base64/decode(data)`

Decodes the given base64 encoded data and return it as a string

##### `encoding/base64/url_encode(data)`

Encodes the given data to an unpadded alternate base64 encoding defined in RFC 4648.

##### `encoding/base64/url_decode(data)`

Decodes the given unpadded alternate base64 encoding defined in RFC 4648 and return it as a string

##### `encoding/hex/encode(value)`

Encode the given value string to hexadecimal values (string)

##### `encoding/hex/decode(value)`

Decode the given hexadecimal string back to the string it represents (UTF-8)

##### `encoding/json/marshal(table)`

Encodes the given table into a JSON string

##### `encoding/json/unmarshal(string)`

Decodes the given string into the equivalent Lua structure

##### `encoding/parquet/get_schema(payload)`

Read the payload (string) as the contents of a Parquet file and return its schema in the following table structure:

```lua
{
  { ["name"] = "column_a", ["type"] = "INT32" },
  { ["name"] = "column_b", ["type"] = "BYTE_ARRAY" }
}
```

##### `lakefs`

The Lua Hook library allows calling back to the lakeFS API using the identity of the user that triggered the action.
For example, if user A tries to commit and triggers a `pre-commit` hook - any call made inside that hook to the lakefs
API, will automatically use user A's identity for authorization and auditing purposes.

##### `lakefs/create_tag(repository_id, reference_id, tag_id)`

Create a new tag for the given reference

##### `lakefs/diff_refs(repository_id, lef_reference_id, right_reference_id [, after, prefix, delimiter, amount])`

Returns an object-wise diff between `left_reference_id` and `right_reference_id`.

##### `lakefs/list_objects(repository_id, reference_id [, after, prefix, delimiter, amount])`

List objects in the specified repository and reference (branch, tag, commit ID, etc.).
If delimiter is empty, will default to a recursive listing. Otherwise, common prefixes up to `delimiter` will be shown as a single entry.

##### `lakefs/get_object(repository_id, reference_id, path)`

Returns 2 values:

1. The HTTP status code returned by the lakeFS API
1. The content of the specified object as a lua string 

##### `path/parse(path_string)`

Returns a table for the given path string with the following structure:

```lua
> require("path")
> path.parse("a/b/c.csv")
{
    ["parent"] = "a/b/"
    ["base_name"] = "c.csv"
} 
```

##### `path/join(*path_parts)`

Receives a variable number of strings and returns a joined string that represents a path:

```lua
> require("path")
> path.join("path/", "to", "a", "file.data")
path/o/a/file.data
```

##### `path/is_hidden(path_string [, seperator, prefix])`

returns a boolean - `true` if the given path string is hidden (meaning it starts with `prefix`) - or if any of its parents start with `prefix`.

```lua
> require("path")
> path.is_hidden("a/b/c") -- false
> path.is_hidden("a/b/_c") -- true
> path.is_hidden("a/_b/c") -- true
> path.is_hidden("a/b/_c/") -- true
```
##### `path/default_separator()`

Returns a constant string (`/`)

##### `regexp/match(pattern, s)`

Returns true if the string `s` matches `pattern`.
This is a thin wrapper over Go's [regexp.MatchString](https://pkg.go.dev/regexp#MatchString){: target="_blank" }.

##### `regexp/quote_meta(s)`

Escapes any meta-characters in string `s` and returns a new string

##### `regexp/compile(pattern)`

Returns a regexp match object for the given pattern

##### `regexp/compiled_pattern.find_all(s, n)`

Returns a table list of all matches for the pattern, (up to `n` matches, unless `n == -1` in which case all possible matches will be returned)

##### `regexp/compiled_pattern.find_all_submatch(s, n)`

Returns a table list of all sub-matches for the pattern, (up to `n` matches, unless `n == -1` in which case all possible matches will be returned).
Submatches are matches of parenthesized subexpressions (also known as capturing groups) within the regular expression, 
numbered from left to right in order of opening parenthesis. 
Submatch 0 is the match of the entire expression, submatch 1 is the match of the first parenthesized subexpression, and so on

##### `regexp/compiled_pattern.find(s)`

Returns a string representing the left-most match for the given pattern in string `s`

##### `regexp/compiled_pattern.find_submatch(s)`

find_submatch returns a table of strings holding the text of the leftmost match of the regular expression in `s` and the matches, if any, of its submatches

##### `strings/split(s, sep)`

returns a table of strings, the result of splitting `s` with `sep`.

##### `strings/trim(s)`

Returns a string with all leading and trailing white space removed, as defined by Unicode

##### `strings/replace(s, old, new, n)`

Returns a copy of the string s with the first n non-overlapping instances of `old` replaced by `new`. 
If `old` is empty, it matches at the beginning of the string and after each UTF-8 sequence, yielding up to k+1 replacements for a k-rune string. 

If n < 0, there is no limit on the number of replacements

##### `strings/has_prefix(s, prefix)`

Returns `true` if `s` begins with `prefix`

##### `strings/has_suffix(s, suffix)`

Returns `true` if `s` ends with `suffix`

##### `strings/contains(s, substr)`

Returns `true` if `substr` is contained anywhere in `s`

##### `time/now()`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00).

##### `time/format(epoch_nano, layout, zone)`

Returns a string representation of the given epoch_nano timestamp for the given Timezone (e.g. `"UTC"`, `"America/Los_Angeles"`, ...)
The `layout` parameter should follow [Go's time layout format](https://pkg.go.dev/time#pkg-constants){: target="_blank" }.

##### `time/format_iso(epoch_nano, zone)`

Returns a string representation of the given `epoch_nano` timestamp for the given Timezone (e.g. `"UTC"`, `"America/Los_Angeles"`, ...)
The returned string will be in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601){: target="_blank" } format.

##### `time/sleep(duration_ns)`

Sleep for `duration_ns` nanoseconds

##### `time/since(epoch_nano)`

Returns the amount of nanoseconds elapsed since `epoch_nano`

##### `time/add(epoch_time, duration_table)`

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

##### `time/parse(layout, value)`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00).
This timestamp will represent date `value` parsed using the `layout` format.

The `layout` parameter should follow [Go's time layout format](https://pkg.go.dev/time#pkg-constants){: target="_blank" }

##### `time/parse_iso(value)`

Returns a `float64` representing the amount of nanoseconds since the unix epoch (01/01/1970 00:00:00 for `value`.
The `value` string should be in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601){: target="_blank" } format

##### `uuid/new()`

Returns a new 128-bit [RFC 4122 UUID](https://www.rfc-editor.org/rfc/rfc4122){: target="_blank" } in string representation. 

#### Example of a Lua hook:
{:.no_toc}

This example will simply print out a JSON representation of the event that occurred:

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
      script: |
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

For the full set of exposed Lua APIs, usage and reasoning is available on the [proposal document](https://github.com/treeverse/lakeFS/blob/master/design/open/zero-deployment-hooks.md)
{: .note }

---