---
layout: default
title: Run lakeFS
description: Running lakeFS is easy. This section covers how to spin up lakeFS using Docker.
parent: Quickstart
nav_order: 10
has_children: false
next: ["Create your first repository", "./repository.html"]
redirect_from: [ "./quickstart/", "quickstart/installing.html", "quickstart/try.html"]
---

# Run lakeFS

{% include learn_only.html %} 


## Running locally with Docker

To run a local lakeFS instance using [Docker](https://docs.docker.com/){:target="_blank"}:

1. Ensure that you have Docker installed on your computer. 

1. Run the following command in your terminal:

   ```bash
   docker run --pull always -p 8000:8000 treeverse/lakefs run --local-settings
   ```

1. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"} in your web browser.

1. [Create your first repository](repository.md) in lakeFS.


Learn more by following this video tutorial:

<iframe width="560" height="315" src="https://www.youtube.com/embed/CIDrHVFnIJY"></iframe>


## Running locally with Docker, connected to an object store

You can alternatively connect a local lakeFS docker container to your cloud storage:

<div class="tabs">
  <ul>
    <li><a href="#on-aws-s3">AWS S3</a></li>
    <li><a href="#on-azure-blob">Azure Blob Storage</a></li>
    <li><a href="#on-google-gcs">Google Cloud Storage</a></li>
    <li><a href="#on-minio">MinIO</a></li>
  </ul> 
  <div markdown="1" id="on-aws-s3">

To modify the local deployment, for example, in order to use your local lakeFS against S3 storage (as opposed to the local storage), run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 \
   -e LAKEFS_BLOCKSTORE_TYPE='s3' \
   -e AWS_ACCESS_KEY_ID='YourAccessKeyValue' \
   -e AWS_SECRET_ACCESS_KEY='YourSecretKeyValue' \
   treeverse/lakefs run --local-settings
   ```

  </div>
  <div markdown="1" id="on-azure-blob">

To modify the local deployment in order to use your local lakeFS against Azure Blob Storage (as opposed to the local storage), run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 \
   -e LAKEFS_BLOCKSTORE_TYPE='azure' \
   -e LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT='YourAzureStorageAccountName' \
   -e LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY='YourAzureStorageAccessKey' \
   treeverse/lakefs run --local-settings
   ```

  </div>
  <div markdown="1" id="on-google-gcs">

To modify the local deployment in order to use your local lakeFS against Google Cloud Storage (as opposed to the local storage), run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 \
   -e LAKEFS_BLOCKSTORE_TYPE='gs' \
   -e LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON='YourGoogleServiceAccountKeyJSON' \
   treeverse/lakefs run --local-settings
   ```
where you will replace ```YourGoogleServiceAccountKeyJSON``` with JSON string that contains your Google service account key.

If you want to use the JSON file that contains your Google service account key instead of JSON string (as in the previous command) then go to the directory where JSON file is stored and run the command with local parameters:
   ```bash
docker run --pull always -p 8000:8000 \
   -v $PWD:/myfiles \
   -e LAKEFS_BLOCKSTORE_TYPE='gs' \
   -e LAKEFS_BLOCKSTORE_GS_CREDENTIALS_FILE='/myfiles/YourGoogleServiceAccountKey.json' \
   treeverse/lakefs run --local-settings
   ```
This command will mount your present working directory (PWD) within the container and will read the JSON file from your PWD.

  </div>
  <div markdown="1" id="on-minio">

To use lakeFS with MinIO (or other S3-compatible object storage), use the following example:

   ```bash
docker run --pull always -p 8000:8000 \
   -e LAKEFS_BLOCKSTORE_TYPE='s3' \
   -e LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE='true' \
   -e LAKEFS_BLOCKSTORE_S3_ENDPOINT='http://<minio_endpoint>' \
   -e LAKEFS_BLOCKSTORE_S3_DISCOVER_BUCKET_REGION='false' \
   -e LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID='<minio_access_key>' \
   -e LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY='<minio_secret_key>' \
   treeverse/lakefs run --local-settings
   ```

  </div>
</div>


## Other methods

You can try lakeFS:

1. Explore End-to-End examples with [lakeFS-sample repository](https://github.com/treeverse/lakeFS-samples).
1. [On Kubernetes](more_quickstart_options.md#on-kubernetes-with-helm).
1. By [running the binary directly](more_quickstart_options.md#using-the-binary).

Feeling ready for production? [Deploy lakeFS on your cloud account](../deploy/index.md).
{: .note }


## Next steps

Now that your lakeFS is running, try [creating a repository](repository.md).
