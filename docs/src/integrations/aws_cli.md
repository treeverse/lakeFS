---
title: AWS CLI
description: This section shows how to use the AWS CLI for AWS S3 to access lakeFS.
---

# Using lakeFS with AWS CLI

lakeFS exposes an S3-compatible API, so you can use the [AWS S3 CLI](https://docs.aws.amazon.com/cli/latest/reference/s3/) to interact with objects in your repositories.

## Configuration

You would like to configure an AWS profile for lakeFS.

To configure the lakeFS credentials, run:

```bash
aws configure --profile lakefs
```

You will be prompted to enter the _AWS Access Key ID_ and the _AWS Secret Access Key_.

It should look like this:

```bash
aws configure --profile lakefs
# output:  
# AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE    
# AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
# Default region name [None]: 
# Default output format [None]:
```


## Path convention

When accessing objects in S3, you will need to use the lakeFS path convention:

```
s3://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT
```

## Usage

After configuring the credentials, this is what a command should look:

```bash
aws s3 --profile lakefs \
    --endpoint-url https://lakefs.example.com \
    ls s3://example-repo/main/example-directory
```

You can use an [alias](#adding-an-alias) to make it shorter and more convenient.

## Examples

### List directory 

```bash 
aws --profile lakefs \
    --endpoint-url https://lakefs.example.com \
    s3 ls s3://example-repo/main/example-directory
```

### Copy from lakeFS to lakeFS

```bash
aws --profile lakefs \
    --endpoint-url https://lakefs.example.com \
    s3 cp s3://example-repo/main/example-file-1 s3://example-repo/main/example-file-2
```

### Copy from lakeFS to a local path

```bash
aws --profile lakefs \
    --endpoint-url https://lakefs.example.com \
    s3 cp s3://example-repo/main/example-file-1 /path/to/local/file
```
### Copy from a local path to lakeFS

```bash
aws --profile lakefs \
    --endpoint-url https://lakefs.example.com \
    s3 cp /path/to/local/file s3://example-repo/main/example-file-1
```
### Delete file 

```bash 
aws --profile lakefs \
    --endpoint-url https://lakefs.example.com \
    s3 rm s3://example-repo/main/example-directory/example-file
```

### Delete directory

```bash 
aws --profile lakefs \
    --endpoint-url https://lakefs.example.com \
    s3 rm s3://example-repo/main/example-directory/ --recursive
```

## Adding an alias

To make the command shorter and more convenient, you can create an alias:

```bash
alias awslfs='aws --endpoint https://lakefs.example.com --profile lakefs'
```

Now, the ls command using the alias will be as follows:

```bash
awslfs s3 ls s3://example-repo/main/example-directory
```
