---
layout: default
title: Copying data with Rclone
description: Rclone is a command line program to sync files and directories between cloud providers. Start copying data using rclone.
parent: Integrations
nav_order: 10
has_children: false
redirect_from: ../using/rclone.html
---
# Copying data with rclone
{: .no_toc }
[Rclone](https://rclone.org/){:target="_blank" .button-clickable} is a command line program to sync files and directories between cloud providers.
To use it with lakeFS, just create an Rclone remote as describe below, and then use it as you would any other Rclone remote.

{% include toc.html %}
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
    [lakefs]
    type = s3
    provider = AWS
    endpoint = https://lakefs.example.com
	no_check_bucket = true
    EOT
    ```

*   Otherwise, also include your lakeFS access key pair in the Rclone configuration file:

    ```shell
    cat <<EOT >> /home/myuser/.config/rclone/rclone.conf
    [lakefs]
    type = s3
    provider = AWS
    env_auth = false
    access_key_id = AKIAIOSFODNN7EXAMPLE
    secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    endpoint = https://lakefs.example.com
	no_check_bucket = true
    EOT
    ```
	
### Option 2: use Rclone interactive config command

Run this command and follow the instructions:
```shell
rclone config
```
Choose AWS S3 as your type of storage, and enter your lakeFS endpoint as your S3 endpoint.
You will have to choose whether you use your environment for authentication (recommended),
or to enter the lakeFS access key pair into the Rclone configuration.  Select "Edit advanced
config" and accept defaults for all values except `no_check_bucket`:
```
If set, don't attempt to check the bucket exists or create it

This can be useful when trying to minimise the number of transactions
rclone does if you know the bucket exists already.

It can also be needed if the user you are using does not have bucket
creation permissions. Before v1.52.0 this would have passed silently
due to a bug.

Enter a boolean value (true or false). Press Enter for the default ("false").
no_check_bucket> yes
```

## Examples

### Syncing your data from S3 to lakeFS

```shell
rclone sync mys3remote://mybucket/path/ lakefs:example-repo/main/path
```

### Syncing a local directory to lakeFS

```shell
rclone sync /home/myuser/path/ lakefs:example-repo/main/path
```
