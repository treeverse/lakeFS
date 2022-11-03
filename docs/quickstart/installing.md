---
layout: default
title: Install lakeFS
description: Installing lakeFS is easy. This section covers how to spin up lakeFS using Docker.
parent: Quickstart
nav_order: 10
has_children: false
next: ["Create your first repository", "./repository.html"]
---

# Install lakeFS
{: .no_toc }

{% include learn_only.html %} 

## Using Docker 
{: .no_toc }

To run a local lakeFS instance using [Docker](https://docs.docker.com/){:target="_blank"}:

1. Ensure that you have Docker installed on your computer. 

1. Run the following command in your terminal:

   ```bash
   docker run --pull always -p 8000:8000 treeverse/lakefs run --local-settings
   ```

1. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"} in your web browser.

1. [Create your first repository](repository.md) in lakeFS.

## Other methods

You can try lakeFS:

1. [On Kubernetes](more_quickstart_options.md#on-kubernetes-with-helm).
1. By [running the binary directly](more_quickstart_options.md#using-the-binary).

## Modifying the local deployment to run against your cloud data

### Using AWS S3 Storage
To modify the local deployment, for example, in order to use your local lakeFS against S3 storage (as opposed to the local storage), run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 -e LAKEFS_BLOCKSTORE_TYPE='s3' -e AWS_ACCESS_KEY_ID='YourAccessKeyValue' -e AWS_SECRET_ACCESS_KEY='YourSecretKeyValue'  treeverse/lakefs run --local-settings
   ```
### Using Azure Blob Storage
To modify the local deployment in order to use your local lakeFS against Azure Blob Storage (as opposed to the local storage), run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 -e LAKEFS_BLOCKSTORE_TYPE='azure' -e LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT='YourAzureStorageAccountName' -e LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY='YourAzureStorageAccessKey' treeverse/lakefs run --local-settings
   ```

### Using Google Cloud Storage
To modify the local deployment in order to use your local lakeFS against Google Cloud Storage (as opposed to the local storage), run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 -e LAKEFS_BLOCKSTORE_TYPE='gs' -e LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON='YourGoogleServiceAccountKeyJSON' treeverse/lakefs run --local-settings
   ```
where you will replace ```YourGoogleServiceAccountKeyJSON``` with JSON string that contains your Google service account key.

If you want to use the JSON file that contains your Google service account key instead of JSON string (as in the previous command) then go to the directory where JSON file is stored and run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 -v $PWD:/myfiles -e LAKEFS_BLOCKSTORE_TYPE='gs' -e LAKEFS_BLOCKSTORE_GS_CREDENTIALS_FILE='/myfiles/YourGoogleServiceAccountKey.json' treeverse/lakefs run --local-settings
   ```
This command will mount your present working directory (PWD) within the container and will read the JSON file from your PWD.


Note using the ```--local-settings``` flag, metadata is being stored locally in the lakeFS container. Therefore, avoid using this flag for production usages.

Follow the video below to quickly spin up a local lakeFS environment.

<iframe width="560" height="315" src="https://www.youtube.com/embed/CIDrHVFnIJY"></iframe>

## Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).
