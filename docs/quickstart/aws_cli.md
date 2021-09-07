---
layout: default 
title: Add Data
description: In this section we will learn how to configure and use AWS CLI to manage data with the lakeFS Server.
parent: Quickstart
nav_order: 30
has_children: false
---

# Add Data
In this section we'll review how to copy files into lakeFS using the AWS CLI.

1. If you don't have the AWS CLI installed, follow the [instructions here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html){:target="_blank"}.
1. Configure a new connection profile using the credentials we generated earlier:

   ```bash
   aws configure --profile local
   # output:
   # AWS Access Key ID [None]: AKIAJVHTOKZWGCD2QQYQ
   # AWS Secret Access Key [None]: ****************************************
   # Default region name [None]:
   # Default output format [None]:
   ```
1. Let's test to see that it works. We'll do that by calling `s3 ls` which should list our repositories for us:
   
   ```bash
   aws --endpoint-url=http://localhost:8000 --profile local s3 ls
   # output:
   # 2021-06-15 13:43:03 example-repo
   ```

1. Great, now let's copy some files. We'll write to the main branch. This is done by prefixing our path with the name of the branch we'd like to read/write from:

   ```bash
   aws --endpoint-url=http://localhost:8000 --profile local s3 cp ./foo.txt s3://example-repo/main/
   # output:
   # upload: ./foo.txt to s3://example-repo/main/foo.txt
   ```

1. Back in the lakeFS UI, we should be able to see our file added to the main branch!

   ![Object Added]({{ site.baseurl }}/assets/img/object_added.png)
   
### Next steps

Now that your repository contains some data, what about using the [lakeFS CLI](lakefs_cli.md) for committing, branching and viewing the history? 