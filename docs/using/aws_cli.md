---
layout: default
title: AWS CLI
parent: Using lakeFS with...
nav_order: 2
has_children: false
---

# Using lakeFS with AWS CLI
{: .no_toc}
The [AWS Command Line Interface](https://aws.amazon.com/cli/) (CLI) is a unified tool to manage your AWS services.
With just one tool to download and configure,
you can control multiple AWS services from the command line and automate them through scripts.


We could use the file commands for S3 to access lakeFS
{:.pb-5 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }

## Configuration

We would like to configure an AWS profile for lakeFS.

In order to configure the lakeFS credentials run:
```
 aws configure --profile lakefs
```
we will be prompted to enter ```AWS Access Key ID``` , ```AWS Secret Access Key``` 

It should look like this:
```
aws configure --profile lakefs
  
AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE    
AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Default region name [None]: 
Default output format [None]:
```


## Path convention
When accessing objects in s3 we will need to use the lakeFS path convention
    ```s3://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT```

## Usage

After configuring the credentials, This is how a command should look:
``` 
aws s3 --profile lakefs \
--endpoint-url https://example.lakefs.io \
ls s3://example-repo/master/example-directory
```

Where endpoint-url should be the same value [configured](../reference/configuration.md#reference)
for ``` gateways.s3.domain_name```.

We could use an [alias](aws_cli.md#lakefs-alias) to make it shorter and more convenient.

## Examples

### List directory 

``` 
aws s3 --profile lakefs \
  --endpoint-url https://s3.dev.lakefs.io \ 
  ls s3://example-repo/master/example-directory
```

### Copy from lakeFS to lakeFS

``` 
aws --profile lakefs \
  --endpoint-url https://example.lakefs.io \
  s3 cp s3://example-repo/master/example-file-1 s3://example-repo/master/example-file-2
```

### Copy from lakeFS to a local path
```
aws --profile lakefs \
  --endpoint-url https://example.lakefs.io \
  s3 cp s3://example-repo/master/example-file-1 /path/to/local/file
```
### Copy from a local path to lakeFS
```
aws --profile lakefs \
  --endpoint-url https://example.lakefs.io \
  s3 cp /path/to/local/file s3://example-repo/master/example-file-1
```
### Delete file 
``` 
aws --profile lakefs \
  --endpoint-url https://example.lakefs.io \
  s3 rm s3://example-repo/master/example-directory/example-file
```

### Delete directory
``` 
aws --profile lakefs \
  --endpoint-url https://example.lakefs.io \
  s3 rm s3://example-repo/master/example-directory/ --recursive
```

## lakeFS alias

In order to make the command shorter and more convenient we can create an alias:

```
alias awslfs='aws --endpoint https://s3.local.lakefs.io --profile lakefs'
```

Now, the ls command using the alias will be:
```
awslfs ls s3://example-repo/master/example-directory
```