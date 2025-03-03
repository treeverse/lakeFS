---
title: Vertex AI
description: How to use Vertex Datasets and gcsfuse with lakeFS
parent: Integrations
---

# Using Vertex AI with lakeFS

Vertex AI lets Google Cloud users Build, deploy, and scale machine learning (ML) models faster, with fully managed ML tools for any use case.

lakeFS Works with Vertex AI by allowing users to create repositories on [GCS Buckets](../howto/deploy/gcp.md), then use either the Dataset API to create managed Datasets on top of lakeFS version, or by automatically exporting lakeFS object versions in a way readable by [Cloud Storage Mounts](https://cloud.google.com/blog/products/ai-machine-learning/cloud-storage-file-system-ai-training). 

{% include toc.html %}

## Using lakeFS with Vertex Managed Datasets

Vertex's [ImageDataset](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ImageDataset) and [VideoDataset](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.VideoDataset) allow creating a dataset by importing a CSV file from gcs (see [`gcs_source`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.ImageDataset#google_cloud_aiplatform_ImageDataset_create)).

This CSV file contains GCS addresses of image files and their corresponding labels.

Since the lakeFS API supports exporting the underlying GCS address of versioned objects, we can generate such a CSV file when creating the dataset:  

```python
#!/usr/bin/env python

# Requirements:
# google-cloud-aiplatform>=1.31.0
# lakefs>=1.0.0

import csv
from pathlib import PosixPath
from io import StringIO

from lakefs
from google.cloud import storage
from google.cloud import aiplatform

# Dataset configuration
lakefs_repo = 'my-repository'
lakefs_ref = 'main'
img_dataset = 'datasets/my-images/'

# Vertex configuration
import_bucket = 'underlying-gcs-bucket'

# lakeFS client with connection details
client = Client(
    host="https://lakefs.example.com",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)


# produce import file for Vertex's SDK
buf = StringIO()
csv_writer = csv.writer(buf)
for obj in lakesf.repository(lakefs_repo).ref(lakefs_ref).objects(prefix=img_dataset):
    p = PosixPath(obj.path)
    csv_writer.writerow((obj.physical_address, p.parent.name))

# spit out CSV
print('Generated path and labels CSV')
buf.seek(0)

# Write it to storage
storage_client = storage.Client()
bucket = storage_client.bucket(import_bucket)
blob = bucket.blob(f'vertex/imports/{lakefs_repo}/{lakefs_ref}/labels.csv')
with blob.open('w') as out:
    out.write(buf.read())

print(f'Wrote CSV to gs://{import_bucket}/vertex/imports/{lakefs_repo}/{lakefs_ref}/labels.csv')

# import in Vertex, as dataset
print('Importing dataset...')
ds = aiplatform.ImageDataset.create(
    display_name=f'{lakefs_repo}_{lakefs_ref}_imgs',
    gcs_source=f'gs://{import_bucket}/vertex/imports/{lakefs_repo}/{lakefs_ref}/labels.csv',
    import_schema_uri=aiplatform.schema.dataset.ioformat.image.single_label_classification,
    sync=True
)
ds.wait()
print(f'Done! {ds.display_name} ({ds.resource_name})')
```

## Using lakeFS with Cloud Storage Fuse

Vertex allows using Google Cloud Storage mounted as a [Fuse Filesystem](https://cloud.google.com/vertex-ai/docs/training/cloud-storage-file-system) as custom input for training jobs.

Instead of having to copy lakeFS files for each version we want to consume, we can create symlinks by using [gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse)'s native [symlink inodes](https://github.com/GoogleCloudPlatform/gcsfuse/blob/v1.0.0/docs/semantics.md#symlink-inodes).

This process can be fully automated by using the example [gcsfuse_symlink_exporter.lua](https://github.com/treeverse/lakeFS/blob/master/examples/hooks/gcsfuse_symlink_exporter.lua) Lua hook.

Here's what we need to do:

1. Upload the example `.lua` file into our lakeFS repository. For this example, we'll put it under `scripts/gcsfuse_symlink_exporter.lua`.
2. Create a new hook definition file and upload to `_lakefs_actions/export_images.yaml`:

```yaml
---
# Example hook declaration: (_lakefs_actions/export_images.yaml):
name: export_images

on:
  post-commit:
    branches: ["main"]
  post-merge:
    branches: ["main"]
  post-create-tag:

hooks:
- id: gcsfuse_export_images
  type: lua
  properties:
    script_path: scripts/export_gcs_fuse.lua  # Path to the script we uploaded in the previous step
    args:
      prefix: "datasets/images/"  # Path we want to export every commit
      destination: "gs://my-bucket/exports/my-repo/"  # Where should we create the symlinks?
      mount:
        from: "gs://my-bucket/repos/my-repo/"  # Symlinks are to a unix-mounted file
        to: "/gcs/my-bucket/repos/my-repo/"    #  This will ensure they point to a location that exists.
      
      # Should be the contents of a valid credentials.json file
      # See: https://developers.google.com/workspace/guides/create-credentials
      # Will be used to write the symlink files
      gcs_credentials_json_string: |
        {
          "client_id": "...",
          "client_secret": "...",
          "refresh_token": "...",
          "type": "..."
        }
```

Done! On the next tag creation or update to the `main` branch, we'll automatically export the lakeFS version of `datasets/images/` to a mountable location.

To consume the symlink-ed files, we can read them normally from the mount:

```python
with open('/gcs/my-bucket/exports/my-repo/branches/main/datasets/images/001.jpg') as f:
    image_data = f.read()
```

Previously exported commits are also readable, if we exported them in the past:

```python
commit_id = 'abcdef123deadbeef567'
with open(f'/gcs/my-bucket/exports/my-repo/commits/{commit_id}/datasets/images/001.jpg') as f:
    image_data = f.read()
```

### Considerations when using lakeFS with Cloud Storage Fuse

For lakeFS paths to be readable by gcsfuse, the mount option `--implicit-dirs` must be specified.

