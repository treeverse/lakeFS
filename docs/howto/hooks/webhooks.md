---
title: Webhooks
parent: Actions and Hooks
grand_parent: How-To
description: Webhooks reference
redirect_from:
   - /hooks/webhooks.html
---

# Webhooks

{% include toc.html %}

A Webhook is a Hook type that sends an HTTP POST request to the configured URL.
Any non 2XX response by the responding endpoint will fail the Hook, cancel the execution of the following Hooks
under the same Action. For `pre-*` hooks, the triggering operation will also be aborted.

**Warning:** You should not use `pre-*` webhooks for long-running tasks, since they block the performed operation.
Moreover, the branch is locked during the execution of `pre-*` hooks, so the webhook server cannot perform any write operations on the branch (like uploading or commits).
{: .note }

## Action File Webhook properties

_See the [Action configuration](./index.md#action-file) for overall configuration schema and details._

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

All environment variables need to begin with "LAKEFSACTIONS_". Otherwise, they will be blocked.
Additionally, the `actions.env.enabled` configuration parameter can be set to `false` to block access to all environment variables.
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

## Request body schema
Upon execution, a webhook will send a request containing a JSON object with the following fields:

| Field               | Description                                                       | Type   |
|---------------------|-------------------------------------------------------------------|--------|
| event_type          | Type of the event that triggered the _Action_                     | string |
| event_time          | Time of the event that triggered the _Action_ (RFC3339 formatted) | string |
| action_name         | Containing _Hook_ Action's Name                                   | string |
| hook_id             | ID of the _Hook_                                                  | string |
| repository_id       | ID of the Repository                                              | string |
| branch_id[^1]       | ID of the Branch                                                  | string |
| source_ref          | Reference to the source on which the event was triggered          | string |
| commit_message[^2]  | The message for the commit (or merge) that is taking place        | string |
| committer[^2]       | Name of the committer                                             | string |
| commit_metadata[^2] | The metadata for the commit that is taking place                  | string |
| commit_id[^2,^4]    | The ID of the commit that is being created              | string |
| tag_id[^3]          | The ID of the created/deleted tag                                 | string |

[^1]: N\A for Tag events  
[^2]: N\A for Tag and Create/Delete Branch events  
[^3]: Applicable only for Tag events
[^4]: Applicable to commit/merge events. For merges, this represents the merge commit ID to be created if the merge operation succeeds.

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
  "commit_id": "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
  "committer": "committer",
  "commit_metadata": {
    "key": "value"
  }
}
```
