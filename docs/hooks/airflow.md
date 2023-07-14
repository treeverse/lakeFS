---
layout: default
title: Airflow Hooks
parent: Actions and Hooks
grand_parent: Reference
description: Airflow Hooks Reference
---

# Airflow Hooks

{% include toc.html %}

Airflow Hook triggers a DAG run in an Airflow installation using [Airflow's REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/post_dag_run).
The hook run succeeds if the DAG was triggered, and fails otherwise.

## Action File Airflow Hook Properties

_See the [Action configuration](./index.md#action-file) for overall configuration schema and details._

| Property      | Description                                                     | Data Type                                                                                 | Example                 | Required | Environment Variables Supported |
|---------------|-----------------------------------------------------------------|-------------------------------------------------------------------------------------------|-------------------------|----------|------------------|
| url           | The URL of the Airflow instance                                 | String                                                                                    | `http://localhost:8080` | true     | no               |
| dag_id        | The DAG to trigger                                              | String                                                                                    | `example_dag`           | true     | no               |
| username      | The name of the Airflow user performing the request             | String                                                                                    | `admin`                 | true     | no               |
| password      | The password of the Airflow user performing the request         | String                                                                                    | `admin`                 | true     | yes              |
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

## Hook Record in configuration field

lakeFS will add an entry to the Airflow request configuration property (`conf`) with the event that triggered the action.

The key of the record will be `lakeFS_event` and the value will match the one described [here](./webhooks.html#request-body-schema)
