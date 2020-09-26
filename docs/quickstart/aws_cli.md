---
layout: default
title: Copy Files
parent: Quick Start
nav_order: 30
has_children: false
---

# Copying files into lakeFS using AWS CLI
1. If you don't have the AWS CLI installed, follow the [instructions here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html){:target="_blank"}.
1. Configure a new connection profile using the credentials we generated earlier:

   ```bash
   aws configure --profile local
   # output:
   # AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
   # AWS Secret Access Key [None]: ****************************************
   # Default region name [None]:
   # Default output format [None]:
   ```
1. Let's test to see that it works. We'll do that by calling `s3 ls` which should list our repositories for us:
   
   ```bash
   aws --endpoint-url=http://s3.local.lakefs.io:8000 --profile local s3 ls
   # output:
   # 2020-05-18 17:47:03 example
   ```
   
   **Note:** We're using `s3.local.lakefs.io` - a special DNS record which always resolves to localhost, subdomains included.  
   Since S3's API uses [subdomains for bucket addressing](https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/){: target="_blank"}, simply using `localhost:8000` as an endpoint URL will not work.
   {: .note .note-info }

1. Great, now let's copy some files. We'll write to the master branch. This is done by prefixing our path with the name of the branch we'd like to read/write from:

   ```bash
   aws --endpoint-url=http://s3.local.lakefs.io:8000 --profile local s3 cp ./foo.txt s3://example/master/
   # output:
   # upload: ./foo.txt to s3://example/master/foo.txt
   ```

1. Back in the lakeFS UI, we should be able to see our file added to the master branch!

   ![Object Added](../assets/img/object_added.png)
   
### Next steps

Now that your repository contains some data, what about using the [lakeFS CLI](lakefs_cli.md) for committing, branching and viewing the history? 