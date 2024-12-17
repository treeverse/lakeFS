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
> Standalone GC is experimental and offers limited capabilities compared to the [Spark-backed GC]({% link howto/garbage-collection/gc.md %}). For large scale environments, we recommend using the Spark-backed solution.

{% include toc_2-3.html %}

## What is Standalone GC?

Standalone GC is a simplified version of the Spark-backed GC that runs without any external dependencies, delivered as a standalone
docker image. It supports S3 and [self-managed S3 compatible storages](#using-s3-compatible-clients) such as MinIO.    

## Limitations

1. **No horizontal scalability**: Only a single instance of `lakefs-sgc` can operate on a given repository at a time.
2. **Mark phase only**: Standalone GC supports only the mark phase, identifying objects for deletion but not executing 
the sweep stage to delete them. It functions similarly to the GC's [mark-only mode]({% link howto/garbage-collection/gc.md %}#mark-only-mode).
3. Only supports AWS S3 and S3-compatible object storages. However, supporting Azure blob and GCS are in our roadmap.

## Installation 

### Step 1: Obtain Dockerhub token

#### lakeFS Enterprise customers 

Contact your account manager to verify that Standalone GC is included in your license. Then use your dockerhub token for 
the `externallakefs` user.

#### New to lakeFS Enterprise

Please [contact us](https://lakefs.io/contact-sales/) to get trial access to Standalone GC.

### Step 2: Login to Dockerhub with this token

```bash
docker login -u <token>
```

### Step 3: Download the docker image

Download the image from the [lakefs-sgc](https://hub.docker.com/repository/docker/treeverse/lakefs-sgc/general) repository:
```bash
docker pull treeverse/lakefs-sgc:<tag>
```

## Setup

### Permissions

To run `lakefs-sgc`, you need both AWS (or S3-compatible) storage and lakeFS user permissions as outlined below:

#### Storage permissions

The minimum required permissions for AWS or S3-compatible storage are:
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
In this example, the repository storage namespace is `s3://some-bucket/some/prefix`.

#### lakeFS permissions

The minimum required permissions for lakeFS are:
```json
{
  "statement": [
    {
      "action": [
        "retention:PrepareGarbageCollectionCommits",
        "retention:PrepareGarbageCollectionUncommitted",
        "fs:ReadConfig",
        "fs:ReadRepository",
        "fs:ListObjects",
        "fs:ReadConfig"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repository>"
    }
  ]
}
```

### Credentials

Standalone GC supports S3 and S3-compatible storage backends and relies on AWS credentials for authentication. To set up
credentials on the `lakefs-sgc` docker container, follow AWS guidelines, such as those outlined in [this guide](https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-configure.html).
For details on how to pass credentials to `lakefs-sgc`, refer to the instructions in [How to Run Standalone GC](#how-to-run-standalone-gc).

### Using S3-compatible clients

`lakefs-sgc` leverages AWS credentials to work seamlessly with S3-compatible storage solutions, such as [MinIO](https://min.io/). 
Follow the steps below to set up and use `lakefs-sgc` with an S3-compatible client:

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

## How to Run Standalone GC?

### Command line reference

#### Flags
- `-c, --config`: config file to use (default is $HOME/.lakefs-sgc.yaml)

#### Commands
**run**

Usage: \
`lakefs-sgc run <repository>`

Flags:
- `--cache-dir`: directory to cache read files (default is `$HOME/.lakefs-sgc/data/`)
- `--parallelism`: number of parallel downloads for metadata files (default 10)
- `--presign`: use pre-signed URLs when downloading/uploading data (recommended) (default true)


To run standalone GC, choose the method you prefer to pass AWS credentials and invoke the commands below.  

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

When working with S3-compatible clients, it's often more convenient to mount the `~/.aws` directory and pass in the desired profile.

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

We recommend starting by backing up the marked objects to a different bucket before deleting them. After ensuring the 
backup is complete, you can proceed to delete the objects directly from the backup location.

Use the following script to backup marked objects to another bucket:
```bash
# Update these variables with your actual values
storage_ns=<storage namespace (s3://...)>
output_bucket=<output bucket (s3://...)>
run_id=<GC run id>

# Download the CSV file
aws s3 cp "$storage_ns/_lakefs/retention/gc/reports/$run_id/deleted.csv" "./run_id-$run_id.csv"

# Move all addresses to the output bucket under the "run_id=$run_id" prefix
cat run_id-$run_id.csv | tail -n +2 | xargs -I {} aws s3 mv "$storage_ns/{}" "$output_bucket/run_id=$run_id/"
```

To delete the marked objects, use the following script:
```bash
# Update these variables with your actual values
output_bucket=<output bucket (s3://...)>
run_id=<GC run id>

aws s3 rm $output_bucket/run_id=$run_id --recursive
```

{: .note }
> Tip: Remember to periodically delete the backups to actually reduce storage costs.

## Lab tests

Standalone GC was tested on the lakeFS setup below.   

#### Repository spec

- 100k objects
- 250 commits
- 100 branches

#### Machine spec

- 4GiB RAM
- 8 CPUs

#### Testing results

- Time: < 5m
- Disk space: 123MB
