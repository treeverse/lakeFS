---
layout: default
title: Copying data with Rclone
parent: Using lakeFS with...
nav_order: 1
has_children: false
---
# Copying data with rclone
{: .no_toc }
[Rclone](https://rclone.org/){:target="_blank"} is a command line program to sync files and directories between cloud providers.
To use it with lakeFS, just create an Rclone remote as describe below, and then use it as you would any other Rclone remote.                                                                                                  

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}
## Creating a remote for lakeFS in Rclone
To add the remote to Rclone, choose one of the following options:
### Option 1: add an entry in your Rclone configuration file
*   Find the path to your Rclone configuration file and copy it for the next step.
    
    ```shell
    rclone config file
    # output:
    # Configuration file is stored at:
    # /home/myuser/.config/rclone/rclone.conf
    ```
    
*   If your lakeFS access key is already set in an AWS profile or environment variables, just run the following command, replacing the endpoint property with your lakeFS endpoint:

    ```shell
    cat <<EOT >> /home/myuser/.config/rclone/rclone.conf
    # output:
    # [lakefs]
    # type = s3
    # provider = AWS
    # endpoint = https://s3.lakefs.example.com
    #
    # EOT
    ```

*   Otherwise, also include your lakeFS access key pair in the Rclone configuration file:

    ```shell
    cat <<EOT >> /home/myuser/.config/rclone/rclone.conf
    # output:
    # [lakefs]
    # type = s3
    # provider = AWS
    # env_auth = false
    # access_key_id = AKIAIOSFODNN7EXAMPLE
    # secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    # endpoint = https://s3.lakefs.example.com
    # EOT
    ```

### Option 2: use Rclone interactive config command

Run this command and follow the instructions:
```shell
rclone config
```
Choose AWS S3 as your type of storage, and enter your lakeFS endpoint as your S3 endpoint.
You will have to choose whether you use your environment for authentication (recommended),
or to enter the lakeFS access key pair into the Rclone configuration.

## Examples

### Syncing your data from S3 to lakeFS

```shell
rclone sync mys3remote://mybucket/path/ lakefs:example-repo/master/path
```

### Syncing a local directory to lakeFS

```shell
rclone sync /home/myuser/path/ lakefs:example-repo/master/path
```
