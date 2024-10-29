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

{% include toc_2-4.html %}

## About

Standalone GC is a limited version of the Spark-backed GC that runs without any external dependencies, as a standalone docker image.

## Limitations

1. Except for the [Lab tests](./standalone-gc.md#lab-tests) performed, there are no further guarantees about the performance profile of the Standalone GC. 
2. Horizontal scale is not supported - Only a single instance of `lakefs-sgc` can operate at a time on a given repository.
3. Standalone GC only marks objects and does not delete them - Equivalent to the GC's [mark only mode]({% link howto/garbage-collection/gc.md %}#mark-only-mode). \
   More about that in the [Get the List of Objects Marked for Deletion](./standalone-gc.md#get-the-list-of-objects-marked-for-deletion) section.

### Lab tests

Repository spec:

- 100k objects
- 250 commits
- 100 branches

Machine spec:
- 4GiB RAM
- 8 CPUs

In this setup, we measured:

- Time: < 5m
- Disk space: 123MB

## Installation

### Step 1: Obtain Dockerhub token
As an enterprise customer, you should already have a dockerhub token for the `externallakefs` user.
If not, contact us at [support@treeverse.io](mailto:support@treeverse.io).

### Step 2: Login to Dockerhub with this token
```bash
docker login -u <token>
```

### Step 3: Download the docker image
Download the image from the [lakefs-sgc](https://hub.docker.com/repository/docker/treeverse/lakefs-sgc/general) repository:
```bash
docker pull treeverse/lakefs-sgc:<tag>
```

## Usage

### Permissions
To run `lakefs-sgc`, you'll need AWS and LakeFS users, with the following permissions:

#### AWS
The minimal required permissions on AWS are:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::some-bucket/some/prefix/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::some-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets"
      ],
      "Resource": [
        "arn:aws:s3:::*"
      ]
    }
  ]
}
```
In this permissions file, the example repository storage namespace is `s3://some-bucket/some/prefix`.

#### LakeFS
The minimal required permissions on LakeFS are:
```json
{
  "statement": [
    {
      "action": [
        "fs:ReadConfig",
        "fs:ReadRepository",
        "retention:PrepareGarbageCollectionCommits",
        "retention:PrepareGarbageCollectionUncommitted",
        "fs:ListObjects",
        "fs:ReadConfig"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repository>"
    }
  ]
}
```
### AWS Credentials
Currently, `lakefs-sgc` does not provide an option to explicitly set AWS credentials. It relies on the hosting machine
to be set up correctly, and reads the AWS credentials from the machine.

This means, you should set up your machine however AWS expects you to set it. \
For example, by following their guide on [configuring the AWS CLI](https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-configure.html).

#### S3-compatible clients
Naturally, this method of configuration allows for `lakefs-sgc` to work with any S3-compatible client (such as [MinIO](https://min.io/)). \
An example setup for working with MinIO:
1. Add a profile to your `~/.aws/config` file:
    ```
   [profile minio]
   region = us-east-1
   endpoint_url = <MinIO URL>
   s3 =
       signature_version = s3v4
    ```

2. Add an access and secret keys to your `~/.aws/credentials` file:
    ```
   [minio]
   aws_access_key_id     = <MinIO access key>
   aws_secret_access_key = <MinIO secret key>
    ```
3. Run the `lakefs-sgc` docker image and pass it the `minio` profile - see [example](./standalone-gc.md#mounting-the-aws-directory) below.

### Configuration
The following configuration keys are available:

| Key                            | Description                                                                                                                                                   | Default value      | Possible values                                         |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------------------------------------------------|
| `logging.format`               | Logs output format                                                                                                                                            | "text"             | "text","json"                                           |
| `logging.level`                | Logs level                                                                                                                                                    | "info"             | "error","warn",info","debug","trace"                    |
| `logging.output`               | Where to output the logs to                                                                                                                                   | "-"                | "-" (stdout), "=" (stderr), or any string for file path |
| `cache_dir`                    | Directory to use for caching data during run                                                                                                                  | ~/.lakefs-sgc/data | string                                                  |
| `aws.max_page_size`            | Max number of items per page when listing objects in AWS                                                                                                      | 1000               | number                                                  |
| `aws.s3.addressing_path_style` | Whether or not to use [path-style](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#path-style-access) when reading objects from AWS | true               | boolean                                                 |
| `lakefs.endpoint_url`          | The URL to the lakeFS installation - should end with `/api/v1`                                                                                                | NOT SET            | URL                                                     |
| `lakefs.access_key_id`         | Access key to the lakeFS installation                                                                                                                         | NOT SET            | string                                                  |
| `lakefs.secret_access_key`     | Secret access key to the lakeFS installation                                                                                                                  | NOT SET            | string                                                  |

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
   export LAKEFS_SGC_LOGGING_LEVEL=info
   ```

Example (minimalistic) config file:
```yaml
logging:
  level: debug
lakefs:
  endpoint_url: https://your.url/api/v1
  access_key_id: <lakeFS access key>
  secret_access_key: <lakeFS secret key>
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

### How to Run Standalone GC

#### Directly passing in credentials parsed from `~/.aws/credentials`

```bash
docker run \
-e AWS_REGION=<region> \
-e AWS_SESSION_TOKEN="$(grep 'aws_session_token' ~/.aws/credentials | awk -F' = ' '{print $2}')" \
-e AWS_ACCESS_KEY_ID="$(grep 'aws_access_key_id' ~/.aws/credentials | awk -F' = ' '{print $2}')" \
-e AWS_SECRET_ACCESS_KEY="$(grep 'aws_secret_access_key' ~/.aws/credentials | awk -F' = ' '{print $2}')" \
-e LAKEFS_SGC_LAKEFS_ENDPOINT_URL=<lakefs endpoint URL> \
-e LAKEFS_SGC_LAKEFS_ACCESS_KEY_ID=<lakefs accesss key> \
-e LAKEFS_SGC_LAKEFS_SECRET_ACCESS_KEY=<lakefs secret key> \
-e LAKEFS_SGC_LOGGING_LEVEL=debug \
treeverse/lakefs-sgc:<tag> run <repository>
```

#### Mounting the `~/.aws` directory

When working with S3-compatible clients, it's often more convenient to mount the ~/.aws` file and pass in the desired profile.

First, change the permissions for `~/.aws/*` to allow the docker container to read this directory:
```bash
chmod 644 ~/.aws/*
```

Then, run the docker image and mount `~/.aws` to the `lakefs-sgc` home directory on the docker container:
```bash
docker run \
--network=host \
-v ~/.aws:/home/lakefs-sgc/.aws \
-e AWS_REGION=us-east-1 \
-e AWS_PROFILE=<profile> \
-e LAKEFS_SGC_LAKEFS_ENDPOINT_URL=<lakefs endpoint URL> \
-e LAKEFS_SGC_LAKEFS_ACCESS_KEY_ID=<lakefs accesss key> \
-e LAKEFS_SGC_LAKEFS_SECRET_ACCESS_KEY=<lakefs secret key> \
-e LAKEFS_SGC_LOGGING_LEVEL=debug \
treeverse/lakefs-sgc:<tag> run <repository>
```
### Get the List of Objects Marked for Deletion
`lakefs-sgc` will write its reports to `<REPOSITORY_STORAGE_NAMESPACE>/_lakefs/retention/gc/reports/<RUN_ID>/`. \
_RUN_ID_ is generated during runtime by the Standalone GC. You can find it in the logs:
```
"Marking objects for deletion" ... run_id=gcoca17haabs73f2gtq0
```

In this prefix, you'll find 2 objects:
- `deleted.csv` - Containing all marked objects in a CSV containing one `address` column. Example:
   ```
   address
   "data/gcnobu7n2efc74lfa5ug/csfnri7n2efc74lfa69g,_e7P9j-1ahTXtofw7tWwJUIhTfL0rEs_dvBrClzc_QE"
   "data/gcnobu7n2efc74lfa5ug/csfnri7n2efc74lfa78g,mKZnS-5YbLzmK0pKsGGimdxxBlt8QZzCyw1QeQrFvFE"
   ...
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

### Delete marked objects

To delete the objects marked by the GC, you'll need to read the `deleted.csv` file, and manually delete each address from AWS.

It is recommended to move all the marked objects to a different bucket instead of deleting them directly.

Here's an example bash script to perform this operation:
```bash
# Change these to your correct values
storage_ns=<storage namespace (s3://...)>
output_bucket=<output bucket (s3://...)>
run_id=<GC run id>

# Download the CSV file
aws s3 cp "$storage_ns/_lakefs/retention/gc/reports/$run_id/deleted.csv" "./run_id-$run_id.csv"

# Move all addresses to the output bucket under the run_id prefix
cat run_id-$run_id.csv | tail -n +2 | head -n 10 | xargs -I {} aws s3 mv "$storage_ns/{}" "$output_bucket/run_id=$run_id/"
```
