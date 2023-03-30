---
layout: default
title: 🧑🏻‍🎓 Learn more about lakeFS
description: Learn more about lakeFS here with links to resources including quickstart, samples, installation guides, and more. 
parent: ⭐ Quickstart ⭐
nav_order: 99
has_children: false
---

# Learn more about lakeFS

The [lakeFS quickstart](/quickstart) is just the beginning of your lakeFS journey 🛣️

Here are some more resources to help you find out more about lakeFS. 

## Connecting lakeFS to your own object storage

Enjoyed the quickstart and want to try out lakeFS against your own data? Here's how to run lakeFS locally as a Docker container locally connecting to an object store. 

<div class="tabs">
  <ul>
    <li><a href="#on-aws-s3">AWS S3</a></li>
    <li><a href="#on-azure-blob">Azure Blob Storage</a></li>
    <li><a href="#on-google-gcs">Google Cloud Storage</a></li>
    <li><a href="#on-minio">MinIO</a></li>
  </ul> 
  <div markdown="1" id="on-aws-s3">

Note: Make sure the Quickstart Docker Compose from the previous steps isn't also running as you'll get a port conflict.
{: .note .note-info }

   ```bash
docker run --pull always -p 8000:8000 \
   -e LAKEFS_BLOCKSTORE_TYPE='s3' \
   -e AWS_ACCESS_KEY_ID='YourAccessKeyValue' \
   -e AWS_SECRET_ACCESS_KEY='YourSecretKeyValue' \
   treeverse/lakefs run --local-settings
   ```

  </div>
  <div markdown="1" id="on-azure-blob">

Note: Make sure the Quickstart Docker Compose from the previous steps isn't also running as you'll get a port conflict.
{: .note .note-info }

   ```bash
docker run --pull always -p 8000:8000 \
   -e LAKEFS_BLOCKSTORE_TYPE='azure' \
   -e LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT='YourAzureStorageAccountName' \
   -e LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY='YourAzureStorageAccessKey' \
   treeverse/lakefs run --local-settings
   ```

  </div>
  <div markdown="1" id="on-google-gcs">

Note: Make sure the Quickstart Docker Compose from the previous steps isn't also running as you'll get a port conflict.
{: .note .note-info }

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

Note: Make sure the Quickstart Docker Compose from the previous steps isn't also running as you'll get a port conflict.
{: .note .note-info }

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

## Deploying lakeFS

Ready to do this thing for real? The deployment guides show you how to deploy lakeFS [locally](/deploy/onprem.html) (including on [Kubernetes](/deploy/onprem.html#k8s)) or on [AWS](/deploy/aws.html), [Azure](/deploy/azure.html), or [GCP](/deploy/gcp.html). 

Alternatively you might want to have a look at [lakeFS Cloud](https://lakefs.cloud/) which provides a fully-managed, SOC-2 compliant, lakeFS service. 

## lakeFS Samples

The [lakeFS Samples](https://github.com/treeverse/lakeFS-samples) GitHub repository includes some excellent examples including: 

* How to implement cross collection consistency on multiple Delta Tables
* Notebooks to show integration of lakeFS with Spark, Python, Delta Lake, Airflow and Hooks.
* Examples of using lakeFS webhooks to run automated data quality checks on different branches.
* Using lakeFS branching features to create dev/test data environments for ETL testing and experimentation.
* Reproducing ML experiments with certainty using lakeFS tags.

## lakeFS Community

lakeFS' community is important to us. Our **guiding principles** are: 

* Fully open, in code and conversation
* We learn and grow together
* Compassion and respect in every interaction

We'd love for you to join [our **Slack group**](https://lakefs.io/slack) and come and introduce yourself on `#say-hello`. Or just lurk and soak up the vibes 😎

If you're interested in getting involved in lakeFS' development head over our [the **GitHub repo**](https://github.com/treeverse/lakeFS) to look at the code and peruse the issues. The comprehensive [contributing](/contributing.html) document should have you covered on next steps but if you've any questions the `#dev` channel on [Slack](https://lakefs.io/slack) will be delighted to help. 

We love speaking at meetups and chatting to community members at them - you can find a list of these [here](https://lakefs.io/community/). 

Finally, make sure to drop by to say hi on [Twitter](https://twitter.com/lakeFS), [Mastodon](https://data-folks.masto.host/@lakeFS), and [LinkedIn](https://www.linkedin.com/company/treeverse/) 👋🏻

## lakeFS Concepts and Internals

We describe lakeFS as "_git for data_" but what does that actually mean? Have a look at the [concepts](/understand/model.html) and [architecture](/understand/architecture.html) guides, as well as the explanation of [how merges are handled](/understand/how/merge.html). To go deeper you might be interested in [the internals of versioning](/understand/how/versioning-internals.htm) and our [internal database structure](/understand/how/kv.html).