---
title: Standalone Garbage Collection
description: Run a limited version of garbage collection without any external dependencies
parent: Garbage Collection
nav_order: 5
grand_parent: How-To
redirect_from:
  - /cloud/standalone-gc.html
---

# Standalone Garbage Collection
{: .d-inline-block }
lakeFS Enterprise
{: .label .label-green }

{: .d-inline-block }
experimental
{: .label .label-red }

{: .note }
> Standalone GC is only available for [lakeFS Enterprise]({% link enterprise/index.md %}).

{: .note .warning }
> Standalone GC is experimental and offers limited capabilities compared to the [Spark-backed GC]({% link howto/garbage-collection/gc.md %}). Read through the [limitations](./standalone-gc.md#limitations) carefully before using it.

{% include toc_2-3.html %}

## About

Standalone GC is a limited version of the Spark-backed GC that runs without any external dependencies, as a standalone docker image.

## Limitations

1. Except for the [Lab tests](./standalone-gc.md#lab-tests) performed, there are no further guarantees about the performance profile of the Standalone GC. 
2. Horizontal scale is not supported - Only a single instance of `lakefs-sgc` can operate at a time on a given repository.
3. It only marks objects and does not delete them - Equivalent to the GC's [mark only mode]({% link howto/garbage-collection/gc.md %}#mark-only-mode). \
   More about that in the [Output](./standalone-gc.md#output) section.

### Lab tests

<TODO: update with final results once ready>

Repository spec:

- 100k objects
- < 200 commits
- 1 branch

Machine spec:
- 4GiB RAM
- 8 CPUs

In this setup, we measured:

- Time: ~1 minute
- Disk space: 120MiB

## Installation

### Step 1: Obtain Dockerhub token
As an enterprise customer, you should already have a dockerhub token for the `externallakefs` user.
If not, contact us at ___ (TODO: add mail/whaterver).

### Step 2: Login to Dockerhub with this token
```bash
docker login -u <your token>
```

### Step 3: Download the docker image
Download the image from the [lakefs-sgc](https://hub.docker.com/repository/docker/treeverse/lakefs-sgc/general) repository:
```bash
docker pull treeverse/lakefs-sgc:tagname
```

## Usage

### AWS Credentials
Currently, `lakefs-sgc` does not provide an option to explicitly set AWS credentials. It relies on the hosting machine
to be set up correctly, and reads the AWS credentials from the machine.

This means, you should set up your machine however AWS expects you to set it. \
For example, by following their guide on [configuring the AWS CLI](https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-configure.html).

### Configuration
The following configuration keys are available:

| Key                        | Description                                                                                        | Default value                  | Possible values                                         |
|----------------------------|----------------------------------------------------------------------------------------------------|--------------------------------|---------------------------------------------------------|
| `logging.format`           | Logs output format                                                                                 | "text"                         | "text","json"                                           |
| `logging.level`            | Logs level                                                                                         | "info"                         | "error","warn",info","debug","trace"                    |
| `logging.output`           | Where to output the logs to                                                                        | "-"                            | "-" (stdout), "=" (stderr), or any string for file path |
| `logging.file_max_size_mb` | Max file size for logs output (relevant only if `logging.output` is set to a file path)            | 102400 (100MiB)                | number                                                  |
| `logging.files_keep`       | Number of files to keep for logs rotation (relevant only if `logging.output` is set to a file path | 100                            | number                                                  |
| `cache_dir`                | Directory to use for caching data during run                                                       | ~/.lakefs-sgc/data             | string                                                  |
| `aws.max_page_size`        | Max number of items per page when listing objects in AWS                                           | not set (AWS defaults to 1000) | number                                                  |
| `objects_min_age`          | Ignore any object that is last modified within this time frame ("cutoff time")                     | "6h"                           | duration                                                |
| `lakefs.endpoint_url`      | The URL to the lakeFS installation - should end with `/api/v1`                                     | NOT SET                        | URL                                                     |
| `lakefs.access_key_id`     | Access key to the lakeFS installation                                                              | NOT SET                        | string                                                  |
| `lakefs.secret_access_key` | Secret access key to the lakeFS installation                                                       | NOT SET                        | string                                                  |


These keys can be provided in the following ways:
1. Config file: Create a YAML file with the keys, each `.` is a new nesting level. \
   For example, `logging.level` will be:
   ```yaml
   logging:
     level: <value> # info,debug...
   ```
   Then, pass it to the program using the `--config path/to/config.yaml` argument.
2. Environment variables: by setting `LAKEFS_SGC_<KEY>`, with uppercase letters and `.`s converted to `_`s. \
   For example `logging.level` will be:
   ```bash
   export LAKEFS_SGC_LOGGING_LEVEL=info`
   ```

Example (minimalistic) config file:
```yaml
logging:
  level: debug
lakefs:
  endpoint_url: https://your.url/api/v1
  access_key_id: <your lakeFS access key>
  secret_access_key: <your lakeFS secret key>
```

### Command line reference

#### Flags:
- `-c, --config`: config file to use (default is $HOME/.lakefs-sgc.yaml)

#### Commands:
**run**

Usage: \
`lakefs-sgc run <repository>`

Flags:
- `--cache-dir`: directory to cache read files and metadataDir (default is $HOME/.lakefs-sgc/data/)
- `--parallelism`: number of parallel downloads for metadataDir (default 10)
- `--presign`: use pre-signed URLs when downloading/uploading data (recommended) (default true)

### Example - docker run command
Here's an example for running the `treeverse/lakefs-sgc` docker image, with AWS credentials parsed from the `~/.aws/credentials` file:

```bash
docker run \
-e AWS_REGION=<region> \
-e AWS_SESSION_TOKEN="$(grep 'aws_session_token' ~/.aws/credentials | awk -F' = ' '{print $2}')" \
-e AWS_ACCESS_KEY_ID="$(grep 'aws_access_key_id' ~/.aws/credentials | awk -F' = ' '{print $2}')" \
-e AWS_SECRET_ACCESS_KEY="$(grep 'aws_secret_access_key' ~/.aws/credentials | awk -F' = ' '{print $2}')" \
-e LAKEFS_SGC_LAKEFS_ENDPOINT_URL=<your lakefs URL> \
-e LAKEFS_SGC_LAKEFS_ACCESS_KEY_ID=<your accesss key> \
-e LAKEFS_SGC_LAKEFS_SECRET_ACCESS_KEY=<your secret key> \
-e LAKEFS_SGC_LOGGING_LEVEL=debug \
treeverse/lakefs-sgc:<version> run <repository>
```

### Output
`lakefs-sgc` will write its reports to `<REPOSITORY_STORAGE_NAMESPACE>/_lakefs/retention/gc/reports/<RUN_ID>/`. \
_RUN_ID_ is generated during runtime by the Standalone GC. You can find it in the logs:
```
"Marking objects for deletion" ... run_id=gcoca17haabs73f2gtq0
```

In this prefix, you'll find 3 objects:
- `deleted.json` - Containing all marked objects in a json format. 
- `deleted.parquet` - Containing all marked objects in a parquet format, with the following schema:
   ```
  ┌─────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
  │ column_name │ column_type │  null   │   key   │ default │  extra  │
  │   varchar   │   varchar   │ varchar │ varchar │ varchar │ varchar │
  ├─────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
  │ address     │ VARCHAR     │ YES     │         │         │         │
  └─────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┘
  ```
- `summary.json` - A small json summarizing the GC run. Example:
```json
{
    "run_id": "gcoca17haabs73f2gtq0",
    "success": true,
    "first_slice": "gcss5tpsrurs73cqi6e0",
    "start_time": "2024-10-27T13:19:26.890099059Z",
    "cutoff_time": "2024-10-27T07:19:26.890099059Z",
    "num_deleted_objects": 33000
}
```

### Deleting marked objects
To delete the objects marked by the GC, you'll need to read the `deleted.parquet` or `deleted.json` files, and manually delete each address from AWS.
