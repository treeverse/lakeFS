---
layout: default
title: AWS CLI
description: This section covers how to use the AWS CLI for AWS S3 to access lakeFS.
parent: Integrations
nav_order: 15
has_children: false
redirect_from: ../using/aws_cli.html
---

# Using lakeFS with AWS CLI

{: .no\_toc} The [AWS Command Line Interface](https://aws.amazon.com/cli/) \(CLI\) is a unified tool to manage your AWS services. With just one tool to download and configure, you can control multiple AWS services from the command line and automate them through scripts.

We could use the file commands for S3 to access lakeFS {:.pb-5 }

## Table of contents

{: .no\_toc .text-delta }

1. TOC

   {:toc .pb-5 }

## Configuration

We would like to configure an AWS profile for lakeFS.

In order to configure the lakeFS credentials run:

```text
aws configure --profile lakefs
```

we will be prompted to enter `AWS Access Key ID` , `AWS Secret Access Key`

It should look like this:

```text
aws configure --profile lakefs
# output:  
# AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE    
# AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
# Default region name [None]: 
# Default output format [None]:
```

## Path convention

When accessing objects in s3 we will need to use the lakeFS path convention

```text
## Usage

After configuring the credentials, This is how a command should look:
```shell 
aws s3 --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  ls s3://example-repo/main/example-directory
```

Where endpoint-url should be the same value [configured](../reference/configuration.md#reference) for `gateways.s3.domain_name`.

We could use an [alias](aws_cli.md#adding-an-alias) to make it shorter and more convenient.

## Examples

### List directory

```text
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 ls s3://example-repo/main/example-directory
```

### Copy from lakeFS to lakeFS

```text
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 cp s3://example-repo/main/example-file-1 s3://example-repo/main/example-file-2
```

### Copy from lakeFS to a local path

```text
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 cp s3://example-repo/main/example-file-1 /path/to/local/file
```

### Copy from a local path to lakeFS

```text
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 cp /path/to/local/file s3://example-repo/main/example-file-1
```

### Delete file

```text
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 rm s3://example-repo/main/example-directory/example-file
```

### Delete directory

```text
aws --profile lakefs \
  --endpoint-url https://s3.lakefs.example.com \
  s3 rm s3://example-repo/main/example-directory/ --recursive
```

## Adding an alias

In order to make the command shorter and more convenient we can create an alias:

```text
alias awslfs='aws --endpoint https://s3.lakefs.example.com --profile lakefs'
```

Now, the ls command using the alias will be:

```text
awslfs s3 ls s3://example-repo/main/example-directory
```

