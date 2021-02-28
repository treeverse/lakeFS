---
layout: default
title: Hooks
parent: Reference
nav_order: 10
has_children: false
---

# Configurable Hooks
Like other version control systems, lakeFS allows the configuration of `Actions` to trigger when predefined events occur.
 
Supported Events:
1. `pre_commit` - Action runs when the commit occurs, before the commit is finalized.
2. `pre_merge` - Action runs when the merge occurs, before the merge is finalized.
 
lakeFS `Actions` are handled per repository and cannot be reused.  
Failure of any `Hook` under any `Action` will result in aborting the lakeFS operation that is taking place.

## Action
An `Action` is a list of `Hooks` with the same trigger configuration.
`Hooks` list under an `Action` is ordered and an `Hook` will only be executed if all previous `Hooks` passed.  

### Hook
An `Hook` is the basic building block of an `Action`. 
Failure of a single `Hook` will stop the execution of the containing `Action` and fail the `Run`. 

### Action file
Schema of the Action file:

|Property          |Description                                            |Required |Default Value
|------------------|-------------------------------------------------------|---------|--------------------------------------|
|name              |Identify the Action file                               |false    | If missing, filename is used instead 
|on                |List of events that will trigger the hooks             |true     |
|on<event>.branches|Glob pattern list of branches that triggers the hooks  |false    | If empty, Action runs on all branches
|hooks             |List of hooks to be executed                           |true     | 
|hook.id           |ID of the hook, must be unique within the `Action`     |true     | 
|hook.type         |Type of the hook (currently only `webhook` supported   |true     | 
|hook.properties   |Hook's specific configuration                          |true     | 

Example:
```yaml
name: Good files check
description: set of checks to verify that branch is good
on:
  pre-commit:
  pre-merge:
    branches:
      - master
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
`Run` is the instantiation of the repo `Action` files when the event occurs.
lakeFS will fetch, parse and filter the repository `Action` files and start to execute the `Hooks` under each `Action`.
All executed `Hooks` (each with `hook_run_id`) exists in the context of that `Run` (`run_id`).   

## How to upload Action files
`Action` files should be uploaded with the prefix `_lakefs_actions/` to the lakeFS repository.
When an actionable event (see Supported Events above) takes place, lakeFS will read all files with prefix `_lakefs_actions/`
in the repository branch where the action occurred. 
A failure to parse an `Action` file will result with the event aborted. 
 
For example, lakeFS will search and execute all matching `Action` files with the prefix `lakefs://repo1/feature-1/_lakefs_actions/` on:
1. Commit to `feature-1` branch on `repo1` repository. 
2. Merge to `master` branch from `feature-1` branch on `repo1` repository.

## Runs API
OpenAPI endpoint exposes the results of `Runs` execution per repository, branch, commit and specific `Action`.
The endpoint also allows to download the execution log of any executed `Hook` under each `Run` for observability.

### Run results objectstore files 
There are 2 types of files that are stored in the metadata section of lakeFS repository with each `Run`:
1. `_lakefs/actions/log/<<runID>>/<<hookRunID>>.log` - Execution log of the specific `Hook` run.
2. `_lakefs/actions/log/<<runID>>/run.manifest` - Manifest with all `Hooks` execution for the run with their results and additional metadata.

## Type of hooks
Currently, there's only a single type of `Hooks` that is supported by lakeFS, the `webhook`.

## Webhooks
A `Webhook` is an `Hook` type that sends an HTTP POST request to the configured URL.
Any non 2XX response by the responding endpoint will fail the `Hook`, cancel the execution of the following `Hooks` 
under the same `Action` and abort the operation that triggered the `Action`.

**Warning:** Actions Run is a blocking operation - users should not use the webhook for long-running tasks (e.g. Running Spark jobs and waiting to completion).
Moreover, since the branch is locked during the execution, any write operation to the branch (like uploading file or committing) by the webhook server is bound to create a deadlock.
{: .note } 

### Action file Webhook properties

|Property          |Description                                            |Required |Default Value
|------------------|-------------------------------------------------------|---------|--------------------------------------|
|url               |The URL address of the request                         |true     |
|timeout           |Time to wait for response before failing the hook      |false    | 1m 
|query_params      |List of query params that will be added to the request |false    |

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
A JSON object with the following fields:

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
