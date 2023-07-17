---
layout: default
title: Actions and Hooks
description: Overview of lakeFS Actions and Hooks
has_children: true  
has_toc: false
nav_order: 29
redirect_from:
  - /reference/hooks.html
  - /hooks.html
  - /hooks/overview.html
---

# Actions and Hooks in lakeFS

{% include toc.html %}

Like other version control systems, lakeFS allows you to configure _Actions_ to trigger when [predefined events](#supported-events) occur. There are numerous uses for Actions, including: 

1. Format Validator:
   A webhook that checks new files to ensure they are of a set of allowed data formats.
1. Schema Validator:
   A webhook that reads new Parquet and ORC files to ensure they don't contain a block list of column names (or name prefixes).
   This is useful for avoiding accidental PII exposure.
1. Integration with external systems:
   Post-merge and post-commit hooks could be used to export metadata about the change to another system. A common example is exporting `symlink.txt` files that allow e.g. [AWS Athena](../integrations/athena.html) to read data from lakeFS.
1. Notifying downstream consumers:
   Running a post-merge hook to trigger an Airflow DAG or to send a Webhook to an API, notifying it of the change that happened

For step-by-step examples of hooks in action check out the [lakeFS Quickstart](/quickstart/actions-and-hooks.html) and the [lakeFS samples repository](https://github.com/treeverse/lakeFS-samples/).

## Overview

An _action_ defines one or more _hooks_ to execute. lakeFS supports three types of hook: 

1. [Lua](./lua.html) - uses an embedded Lua VM
1. [Webhook](./webhooks.html) - makes a REST call to an external URL
1. [Airflow](./airflow.html) - triggers a DAG in Airflow

"Before" hooks must run successfully before their action. If the hook fails, it aborts the action. Lua hooks and Webhooks are synchronous, and lakeFS waits for them to run to completion. Airflow hooks are asynchronous: lakeFS stops waiting as soon as Airflow accepts triggering the DAG.

## Configuration

There are two parts to configuration an Action: 

1. Create an Action file and upload it to the lakeFS repository
2. Configure the hook(s) that you specified in the Action file. How these are configured will depend on the type of hook. 

### Action files

An **Action** is a list of Hooks with the same trigger configuration, i.e. an event will trigger all Hooks under an Action or none at all.

The Hooks under an Action are ordered and so is their execution.

Before each hook execution the `if` boolean expression is evaluated. The expression can use the functions `success()` and `failure()`, which return true if the hook's actions succeeded or failed, respectively.

By default, when `if` is empty or omitted, the step will run only if no error occurred (the same as `success()`).

#### Action File schema

| Property           | Description                                               | Data Type  | Required | Default Value                                                           |
|--------------------|-----------------------------------------------------------|------------|----------|-------------------------------------------------------------------------|
| `name               `| Identifes the Action file                                  | String     | false    | Action filename                                    |
| `on                 `| List of events that will trigger the hooks                | List       | true     |                                                                         |
| `on<event>.branches `| Glob pattern list of branches that triggers the hooks     | List       | false    | **Not applicable to Tag events.** If empty, Action runs on all branches |
| `hooks              `| List of hooks to be executed                              | List       | true     |                                                                         |
| `hook.id            `| ID of the hook, must be unique within the action.         | String     | true     |                                                                         |
| `hook.type          `| Type of the hook ([types](#hook-types))                   | String     | true     |                                                                         |
| `hook.description   `| Optional description for the hook                         | String     | false    |                                                                         |
| `hook.if            `| Expression that will be evaluated before execute the hook | String     | false    | No value is the same as evaluate `success()`                            |
| `hook.properties    `| Hook's specific configuration, see [Lua](./lua.md#action-file-lua-hook-properties), [WebHook](./webhooks.md#action-file-webhook-properties), and [Airflow](./airflow.md#action-file-airflow-hook-properties) for details                             | Dictionary | true     |                                                                         |

#### Example Action File

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
  - id: alert
    type: webhook
    if: failure()
    description: notify alert system when check failed
    properties:
       url: "https://your.domain.io/alert"
       query_params:
          title: good files webhook failed
  - id: notification
    type: webhook
    if: true
    description: notify that will always run - no matter if one of the previous steps failed
    properties:
       url: "https://your.domain.io/notification"
       query_params:
          title: good files completed
```

**Note:** lakeFS will validate action files only when an **Event** has occurred. <br/>
Use `lakectl actions validate <path>` to validate your action files locally.
{: .note }


### Uploading Action files

Action files should be uploaded with the prefix `_lakefs_actions/` to the lakeFS repository.
When an actionable event (see Supported Events above) takes place, lakeFS will read all files with prefix `_lakefs_actions/`
in the repository branch where the action occurred.
A failure to parse an Action file will result with a failing Run.

For example, lakeFS will search and execute all the matching Action files with the prefix `lakefs://example-repo/feature-1/_lakefs_actions/` on:
1. Commit to `feature-1` branch on `example-repo` repository.
1. Merge to `main` branch from `feature-1` branch on `repo1` repository.


## Supported Events

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

## Runs API & CLI

A **Run** is an instantiation of the repository's Action files when the triggering event occurs.
For example, if your repository contains a pre-commit hook, every commit would generate a Run for that specific commit.

lakeFS will fetch, parse and filter the repository Action files and start to execute the Hooks under each Action.
All executed Hooks (each with `hook_run_id`) exist in the context of that Run (`run_id`).

The [lakeFS API](../reference/api.html) and [lakectl](../reference/cli.html#lakectl-actions) expose the results of executions per repository, branch, commit, and specific Action.
The endpoint also allows to download the execution log of any executed Hook under each Run for observability.


## Result Files

The metadata section of lakeFS repository with each Run contains two types of files:
1. `_lakefs/actions/log/<runID>/<hookRunID>.log` - Execution log of the specific Hook run.
1. `_lakefs/actions/log/<runID>/run.manifest` - Manifest with all Hooks execution for the run with their results and additional metadata.

**Note:** Metadata section of a lakeFS repository is where lakeFS keeps its metadata, like commits and metaranges.
Metadata files stored in the metadata section aren't accessible like user stored files.
{: .note }
