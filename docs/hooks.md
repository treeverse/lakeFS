---
layout: default
title: Hooks
nav_order: 7
has_children: false
---

# Configurable Hooks
Like other version control systems, lakeFS allows the configuration of `Actions` to trigger when predefined events occur.
 
Supported Events:
1. `pre_commit` - Action runs when the commit occurs, before the commit is finalized.
2. `pre_merge` - Action runs when the merge occurs, before the merge is finalized.
 
lakeFS `Actions` are handled per repository and cannot be shared between repositories.  
Failure of any `Hook` under any `Action` of a `pre_*` event will result in aborting the lakeFS operation that is taking place.

`Hooks` are managed by `Action` files that are written to a prefix in the lakeFS repository. 
This allows configuration-as-code inside lakeFS, where `Action` files are declarative and written in YAML.

## Example use-cases
1. Format Validator:
   A webhook that checks new files to ensure they are of a set of allowed data format.
2. Schema Validator:
   A webhook that reads new Parquet and ORC files to ensure they don't contain a block list of column names (or name prefixes). 
   This is useful when we want to avoid accidental PII exposure.

For more examples and configuration samples, check out [lakeFS-hooks](https://github.com/treeverse/lakeFS-hooks) example repo.

## Action
An `Action` is a list of `Hooks` with the same trigger configuration, i.e. an event will trigger all `Hooks` under an `Action`, or none at all.
The `Hooks` under an `Action` are ordered and so is their execution. A `Hook` will only be executed if all previous `Hooks` that were triggered with it, had passed.

### Hook
A `Hook` is the basic building block of an `Action`. 
Failure of a single `Hook` will stop the execution of the containing `Action` and fail the `Run`. 

### Action file
Schema of the Action file:

|Property          |Description                                            |Data Type |Required |Default Value
|------------------|-------------------------------------------------------|----------|---------|--------------------------------------|
|name              |Identify the Action file                               |String    |false    | If missing, filename is used instead 
|on                |List of events that will trigger the hooks             |List      |true     |
|on<event>.branches|Glob pattern list of branches that triggers the hooks  |List      |false    | If empty, Action runs on all branches
|hooks             |List of hooks to be executed                           |List      |true     | 
|hook.id           |ID of the hook, must be unique within the `Action`     |String    |true     | 
|hook.type         |Type of the hook (currently only `webhook` supported   |String    |true     | 
|hook.properties   |Hook's specific configuration                          |Dictionary|true     | 

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

## Run
A `Run` is an instantiation of the repository's `Action` files when the triggering event occurs. 
For example, if our repository contains a pre-commit hook, every commit would generate a `Run` for that specific commit.
 
lakeFS will fetch, parse and filter the repository `Action` files and start to execute the `Hooks` under each `Action`.
All executed `Hooks` (each with `hook_run_id`) exists in the context of that `Run` (`run_id`).   

## How to upload Action files
`Action` files should be uploaded with the prefix `_lakefs_actions/` to the lakeFS repository.
When an actionable event (see Supported Events above) takes place, lakeFS will read all files with prefix `_lakefs_actions/`
in the repository branch where the action occurred. 
A failure to parse an `Action` file will result with a failing `Run`. 
 
For example, lakeFS will search and execute all matching `Action` files with the prefix `lakefs://repo1/feature-1/_lakefs_actions/` on:
1. Commit to `feature-1` branch on `repo1` repository. 
2. Merge to `main` branch from `feature-1` branch on `repo1` repository.

## Runs API & CLI
[OpenAPI](reference/api.md) endpoint and [lakectl](reference/commands.md/#lakectl-actions) expose the results of `Runs` execution per repository, branch, commit and specific `Action`.
The endpoint also allows to download the execution log of any executed `Hook` under each `Run` for observability.


### Result Files 
There are 2 types of files that are stored in the metadata section of lakeFS repository with each `Run`:
1. `_lakefs/actions/log/<runID>/<hookRunID>.log` - Execution log of the specific `Hook` run.
2. `_lakefs/actions/log/<runID>/run.manifest` - Manifest with all `Hooks` execution for the run with their results and additional metadata.

**Note:** Metadata section of a lakeFS repository is where lakeFS keeps its metadata, like commits and metaranges.
Metadata files stored in the metadata section aren't accessible like user stored files.
{: .note }

## Type of hooks
Currently, only a single type of `Hooks` is supported by lakeFS: `webhook`.

## Webhooks
A `Webhook` is a `Hook` type that sends an HTTP POST request to the configured URL.
Any non 2XX response by the responding endpoint will fail the `Hook`, cancel the execution of the following `Hooks` 
under the same `Action` and abort the operation that triggered the `Action`.

**Warning:** Actions Run is a blocking operation - users should not use the webhook for long-running tasks (e.g. Running Spark jobs and waiting to completion).
Moreover, since the branch is locked during the execution, any write operation to the branch (like uploading file or committing) by the webhook server is bound to fail.
{: .note } 

### Action file Webhook properties

|Property          |Description                                            |Data Type                                                                                |Required |Default Value
|------------------|-------------------------------------------------------|-----------------------------------------------------------------------------------------|---------|--------------------------------------|
|url               |The URL address of the request                         |String                                                                                   |true     |
|timeout           |Time to wait for response before failing the hook      |String (golang's [Duration](https://golang.org/pkg/time/#Duration.String) representation)|false    | 1m 
|query_params      |List of query params that will be added to the request |Dictionary(String:String or String:List(String)                                          |false    |

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
...
```

### Request body schema
Upon execution, a webhook will send a request containing a JSON object with the following fields:

|Field             |Description                                                                          |Type  |Example                  |
|------------------|-------------------------------------------------------------------------------------|------|-------------------------|
|EventType         |Type of the event that triggered the `Action`                                        |string|pre_commit               |
|EventTime         |Time of the event that triggered the `Action` (RFC3339)                              |string|2006-01-02T15:04:05Z07:00|
|ActionName        |Containing `Hook` Action's Name                                                        |string|                         |
|HookID            |ID of the `Hook`                                                                       |string|                         |
|RepositoryID      |ID of the Repository                                                                 |string|
|BranchID          |ID of the Branch                                                                     |string|
|SourceRef         |Reference to the source that triggered the event (source Branch for commit or merge) |string|
|CommitMessage     |The message for the commit (or merge) that is taking place                           |string|
|Committer         |Name of the committer                                                                |string|
|CommitMetadata    |The metadata for the commit that is taking place                                     |string|

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
