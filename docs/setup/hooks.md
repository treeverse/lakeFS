---
layout: default
title: Hooks
parent: Set up lakeFS
description: lakeFS allows you to configure hooks to trigger when predefined events occur.
nav_order: 30
has_children: false
redirect_from: ../hooks.html
---
# Configurable Hooks
{: .no_toc }

{% include toc.html %}

Like other version control systems, lakeFS allows you to configure `Actions` to trigger when predefined events occur.

Supported Events:

| Event                | Description                                                                    |
|----------------------|--------------------------------------------------------------------------------|
| `pre_commit`         | Runs when the commit occurs, before the commit is finalized                    |
| `post_commit`        | Runs after the commit is finalized                                             |
| `pre_merge`          | Runs on the source branch when the merge occurs, before the merge is finalized |
| `post_merge`         | Runs on the merge result, after the merge is finalized                         |
| `pre_create_branch`  | Runs on the source branch prior to creating a new branch                       |
| `post_create_branch` | Runs on the new branch after the branch was created                            |
| `pre_delete_branch`  | Runs prior to deleting a branch                                                |
| `post_delete_branch` | Runs after the branch was deleted                                              |
| `pre_create_tag`     | Runs prior to creating a new tag                                               |
| `post_create_tag`    | Runs after the tag was created                                                 |
| `pre_delete_tag`     | Runs prior to deleting a tag                                                   |
| `post_delete_tag`    | Runs after the tag was deleted                                                 |

lakeFS `Actions` are handled per repository and cannot be shared between repositories.
A failure of any `Hook` under any `Action` of a `pre_*` event will result in aborting the lakeFS operation that is taking place.
`Hook` failures under any `Action` of a `post_*` event will not revert the operation.

`Hooks` are managed by `Action` files that are written to a prefix in the lakeFS repository.
This allows configuration-as-code inside lakeFS, where `Action` files are declarative and written in YAML.

## Example use cases

1. Format Validator:
   A webhook that checks new files to ensure they are of a set of allowed data format.
1. Schema Validator:
   A webhook that reads new Parquet and ORC files to ensure they don't contain a block list of column names (or name prefixes).
   This is useful for avoiding accidental PII exposure.

For more examples and configuration samples, check out [lakeFS-hooks](https://github.com/treeverse/lakeFS-hooks) example repo.

## Terminology

### Action
{: .no_toc }

An `Action` is a list of `Hooks` with the same trigger configuration, i.e. an event will trigger all `Hooks` under an `Action` or none at all.
The `Hooks` under an `Action` are ordered and so is their execution. A `Hook` will only be executed if all the previous `Hooks` that were triggered with it had passed.

### Hook
{: .no_toc }

A `Hook` is the basic building block of an `Action`.
The failure of a single `Hook` will stop the execution of the containing `Action` and fail the `Run`.

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

**Note:** lakeFS will validate action files only when an `Event` has occurred. <br/>
Use `lakectl actions validate <path>` to validate your action files locally.
{: .note }

### Run
{: .no_toc }

A `Run` is an instantiation of the repository's `Action` files when the triggering event occurs.
For example, if your repository contains a pre-commit hook, every commit would generate a `Run` for that specific commit.

lakeFS will fetch, parse and filter the repository `Action` files and start to execute the `Hooks` under each `Action`.
All executed `Hooks` (each with `hook_run_id`) exist in the context of that `Run` (`run_id`).

---
## Uploading Action files

`Action` files should be uploaded with the prefix `_lakefs_actions/` to the lakeFS repository.
When an actionable event (see Supported Events above) takes place, lakeFS will read all files with prefix `_lakefs_actions/`
in the repository branch where the action occurred.
A failure to parse an `Action` file will result with a failing `Run`.

For example, lakeFS will search and execute all the matching `Action` files with the prefix `lakefs://repo1/feature-1/_lakefs_actions/` on:
1. Commit to `feature-1` branch on `repo1` repository.
1. Merge to `main` branch from `feature-1` branch on `repo1` repository.

## Runs API & CLI

The [lakeFS API](../reference/api.md) and [lakectl](../reference/commands.md#lakectl-actions) expose the results of executions per repository, branch, commit, and specific `Action`.
The endpoint also allows to download the execution log of any executed `Hook` under each `Run` for observability.


### Result Files
{: .no_toc }

The metadata section of lakeFS repository with each `Run` contains two types of files:
1. `_lakefs/actions/log/<runID>/<hookRunID>.log` - Execution log of the specific `Hook` run.
1. `_lakefs/actions/log/<runID>/run.manifest` - Manifest with all `Hooks` execution for the run with their results and additional metadata.

**Note:** Metadata section of a lakeFS repository is where lakeFS keeps its metadata, like commits and metaranges.
Metadata files stored in the metadata section aren't accessible like user stored files.
{: .note }

---
## Hook types

Currently, there are two types of `Hooks` that are supported by lakeFS: [Webhook](#webhooks) and [Airflow](#airflow-hooks).

### Webhooks

A `Webhook` is a `Hook` type that sends an HTTP POST request to the configured URL.
Any non 2XX response by the responding endpoint will fail the `Hook`, cancel the execution of the following `Hooks` 
under the same `Action`. For `pre_*` hooks, the triggering operation will also be aborted.

**Warning:** You should not use `pre_*` webhooks for long-running tasks, since they block the performed operation.
Moreover, the branch is locked during the execution of `pre_*` hooks, so the webhook server cannot perform any write operations on the branch (like uploading or commits).
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
The format `{% raw %}{{{% endraw %} ENV.SOME_ENV_VAR {% raw %}}}{% endraw %}` will be replaced with the value of `SOME_ENV_VAR`
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

| Field               | Description                                                | Type   |
|---------------------|------------------------------------------------------------|--------|
| event_type          | Type of the event that triggered the `Action`              | string |
| event_time          | Time of the event that triggered the `Action` (RFC3339)    | string |
| action_name         | Containing `Hook` Action's Name                            | string |
| hook_id             | ID of the `Hook`                                           | string |
| repository_id       | ID of the Repository                                       | string |
| branch_id[^1]       | ID of the Branch                                           | string |
| source_ref          | Reference to the source on which the event was triggered   | string |
| commit_message[^2]  | The message for the commit (or merge) that is taking place | string |
| committer[^2]       | Name of the committer                                      | string |
| commit_metadata[^2] | The metadata for the commit that is taking place           | string |
| tag_id[^3]          | The ID of the created/deleted tag                          | string |

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
| timeout       | Time to wait for the DAG run to complete (default: 1m)        | String (golang's [Duration](https://golang.org/pkg/time/#Duration.String) representation) |                         | false    | no               |

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

---
## Get started with your first lakeFS webhook

To configure your first pre-merge webhook with lakeFS in 10 minutes, clone the [lakeFS-samples](https://github.com/treeverse/lakeFS-samples) repository and follow along the steps in the [readme](https://github.com/treeverse/lakeFS-samples/tree/main/04-data-quality-checks-with-lakeFS-hooks#running-data-quality-checks-with-lakefs-hooks).

## Experimentation

Sometimes it may be easier to start experimenting with lakeFS webhooks, even before you have a running server to receive the calls.
There are a couple of online tools that can intercept and display the webhook requests, and one of them is Svix.

1. Go to [play.svix.com](https://play.svix.com) and copy the URL address supplied by Svix.
It should look like `https://api.relay.svix.com/api/v1/play/receive/<Random_Gen_String>/`

1. Upload the following action file to lakeFS under the path `_lakefs_actions/test.yaml` in the default branch:

   ```yaml
   name: Sending everything to Svix
   description: Experimenting with webhooks
   on:
      pre-commit:
         branches:
      pre-merge:
         branches:
      post-commit:
         branches:
      post-merge:
         branches:
   hooks:
      - id: svix
        type: webhook
        properties:
           url: "https://api.relay.svix.com/api/v1/play/receive/<Random_Gen_String>/"
   ```

   by using:

   ```bash
      lakectl fs upload lakefs://example-repo/main/_lakefs_actions/test.yaml -s path/to/action/file
   ```

   or the UI.

1. Commit that file to the branch.

   ```bash
      lakectl commit lakefs://example-repo/main -m 'added webhook action file'
   ```

1. Every time you commit or merge to a branch, the relevant `pre_*` and `post_*` requests will be available
in the Svix endpoint you provided. You can also check the `Actions` tab in the lakeFS UI for more details.

![Setup]({{ site.baseurl }}/assets/img/svix_play.png)

---
