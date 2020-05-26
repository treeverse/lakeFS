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

We would like to configure an AWS profile for lakeFS 

### Configure credentials
First of all lets configure the lakeFS credentials
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
### Configure endpoint

Let's also configure the endpoint-url to be our lakeFS endpoint ```example.lakefs.io```

```
aws configure --profile lakefs  set endpoint-url example.lakefs.io
```

## Path convention
When accessing objects in s3 we will need to use the lakeFS path convention
    ```s3://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT```

## Examples

### List directory 

``` 
aws s3 --profile lakefs ls s3://example-repo/master/example-directory
```

### Copy from lakeFS to lakeFS

``` 
aws s3 --profile lakefs cp s3://example-repo/master/example-file-1 s3://example-repo/master/example-file-2
```

### Copy from lakeFS to a local path
```
aws s3 --profile lakefs cp s3://example-repo/master/example-file-1 /path/to/local/file
```
### Copy from a local path to lakeFS
```
aws s3 --profile lakefs cp /path/to/local/file s3://example-repo/master/example-file-1
```
### Delete file 
``` 
aws s3 --profile lakefs rm s3://example-repo/master/example-directory/example-file
```

### Delete directory
``` 
aws s3 --profile lakefs rm s3://example-repo/master/example-directory/ --recursive
```
