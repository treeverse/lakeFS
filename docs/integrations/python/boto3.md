---
title: Using Boto3 with lakeFS S3 Gateway
description: Connect to lakeFS using Boto3 (the AWS SDK for Python) via its S3-compatible gateway.
parent: Python Integration
grand_parent: Integrations
---

# Using Boto3 with the lakeFS S3 Gateway

{% raw %}{% include toc.html %}{% endraw %}

## Overview

lakeFS exposes an S3-compatible API gateway, allowing you to use AWS tools like Boto3 (the AWS SDK for Python) to interact with objects stored in your lakeFS repositories. This is particularly useful if you have existing scripts or applications that use Boto3 for S3 and you want to adapt them to work with lakeFS.

### When to Use Boto3 with lakeFS

*   You have existing Python scripts or applications that use Boto3 to interact with S3.
*   You prefer using the Boto3 interface for object operations (put, get, list, delete).
*   You want to perform S3-style operations on lakeFS branches or commit IDs.

**Important Note:** While Boto3 can be used for object operations (CRUD on data), it **cannot** perform lakeFS-specific version control operations like `commit`, `merge`, `branch`, or `tag`. For these operations, you should use the [High-Level Python SDK (`lakefs`)](./high_level_sdk.md) or the [Generated Python SDK (`lakefs_sdk`)](./generated_sdk.md).

### Initializing the Boto3 S3 Client

To use Boto3 with lakeFS, you need to configure an S3 client by specifying the lakeFS server's S3 gateway endpoint and your lakeFS access credentials.

```python
import boto3
from botocore.config import Config # Needed for advanced configuration like checksums

# --- Configuration ---
# IMPORTANT: Replace these placeholders with your actual lakeFS endpoint and credentials.
LAKEFS_S3_ENDPOINT = 'http://your-lakefs-endpoint:8000' # Use 'https://...' if HTTPS is configured
LAKEFS_ACCESS_KEY_ID = 'YOUR_LAKEFS_ACCESS_KEY_ID'
LAKEFS_SECRET_ACCESS_KEY = 'YOUR_LAKEFS_SECRET_ACCESS_KEY'

# Standard Boto3 S3 client configuration for HTTP
s3_client_http = boto3.client(
    's3',
    endpoint_url=LAKEFS_S3_ENDPOINT,
    aws_access_key_id=LAKEFS_ACCESS_KEY_ID,
    aws_secret_access_key=LAKEFS_SECRET_ACCESS_KEY,
    # region_name is not strictly required for lakeFS but can be set if your tools expect it.
    # region_name='us-east-1' 
)
print(f"Boto3 S3 client (HTTP) configured for lakeFS endpoint: {LAKEFS_S3_ENDPOINT}")

# If using HTTPS, checksum configuration is often necessary:
LAKEFS_S3_ENDPOINT_HTTPS = 'https://your-lakefs-endpoint.example.com' # Ensure this is your HTTPS endpoint

s3_config_https = Config(
    s3={'addressing_style': 'path'}, # 'path' style is often recommended for S3-compatible services
    signature_version='s3v4',      # Recommended signature version
    # Crucial for HTTPS with newer Boto3 versions to avoid checksum errors with lakeFS:
    request_checksum_calculation='WHEN_REQUIRED',
    response_checksum_validation='WHEN_REQUIRED'
)

s3_client_https = boto3.client(
    's3',
    endpoint_url=LAKEFS_S3_ENDPOINT_HTTPS,
    aws_access_key_id=LAKEFS_ACCESS_KEY_ID,
    aws_secret_access_key=LAKEFS_SECRET_ACCESS_KEY,
    config=s3_config_https
)
print(f"Boto3 S3 client (HTTPS with checksum config) configured for lakeFS endpoint: {LAKEFS_S3_ENDPOINT_HTTPS}")
```
Choose the appropriate client (`s3_client_http` or `s3_client_https`) for subsequent operations based on your lakeFS server setup. The examples below will use a generic `s3_client` variable; ensure it's assigned to one of these configured clients.

#### Checksum Configuration Explained (for HTTPS)

Newer Boto3 versions (typically `boto3 >= 1.28.38`, `botocore >= 1.31.38`) changed how they handle checksums with S3-compatible services. When using HTTPS with lakeFS, this can lead to `AccessDenied` errors during uploads, often with server logs showing `encoding/hex: invalid byte: U+0053 'S'`.

The `request_checksum_calculation='WHEN_REQUIRED'` and `response_checksum_validation='WHEN_REQUIRED'` settings in `botocore.config.Config` instruct Boto3 to align with server expectations regarding MD5 checksums, which helps prevent these errors with lakeFS.

### Using with lakeFS and AWS S3 Simultaneously (Boto S3 Router)

If your application needs to interact with both lakeFS and AWS S3 using Boto3, the [**Boto S3 Router**](https://github.com/treeverse/boto-s3-router) (a community tool) can simplify this. It patches Boto3 to route requests to different S3 endpoints based on the bucket name.

{: .note }
> Refer to the [Boto S3 Router GitHub repository](https://github.com/treeverse/boto-s3-router) for its installation and usage details.

## Tutorials / User Guides

In Boto3, a lakeFS repository maps to an S3 `Bucket`, and an object path within a lakeFS branch or commit ID maps to an S3 `Key`. The following examples assume `s3_client` is one of the clients configured in the "Initialization" section.

### Uploading an Object (PutObject)

To upload a file, specify the repository name as `Bucket` and `branch_name/path/to/object` as `Key`.

```python
import os
import boto3 # Assuming boto3 is imported
from botocore.exceptions import ClientError # For Boto3 specific error handling

# Assume s3_client is configured as shown in the Overview section.
# For example, if using HTTP:
# s3_client = s3_client_http 
# Or if using HTTPS:
# s3_client = s3_client_https

# Replace with your actual client if running this snippet standalone:
# LAKEFS_S3_ENDPOINT = 'http://your-lakefs-endpoint:8000' 
# LAKEFS_ACCESS_KEY_ID = 'YOUR_LAKEFS_ACCESS_KEY_ID'
# LAKEFS_SECRET_ACCESS_KEY = 'YOUR_LAKEFS_SECRET_ACCESS_KEY'
# s3_client = boto3.client('s3', endpoint_url=LAKEFS_S3_ENDPOINT, 
#                          aws_access_key_id=LAKEFS_ACCESS_KEY_ID, 
#                          aws_secret_access_key=LAKEFS_SECRET_ACCESS_KEY)


repo_name_boto = "my-boto3-ops-repo" 
branch_name_boto = "main"            
object_key_boto = f"{branch_name_boto}/uploads/sample_via_boto.txt" 
local_file_to_upload = "local_boto_sample.txt" 

with open(local_file_to_upload, 'w') as f:
    f.write("Hello lakeFS from Boto3 example!")

try:
    with open(local_file_to_upload, 'rb') as f_data:
        s3_client.put_object(
            Bucket=repo_name_boto,
            Key=object_key_boto,
            Body=f_data
            # You can also add 'ContentType': 'text/plain', 'Metadata': {'myKey': 'myValue'}
        )
    print(f"Successfully uploaded '{local_file_to_upload}' to '{repo_name_boto}/{object_key_boto}'")
    print(f"Note: This is an uncommitted change on branch '{branch_name_boto}'. Commit using lakeFS tools.")
except ClientError as e:
    error_code = e.response.get('Error', {}).get('Code')
    print(f"Boto3 ClientError during upload: {error_code} - {e}")
    print(f"Ensure repository '{repo_name_boto}' exists and credentials/endpoint are correct.")
except Exception as e:
    print(f"An unexpected error occurred during upload: {e}")
finally:
    if os.path.exists(local_file_to_upload):
        os.remove(local_file_to_upload)
```

### Listing Objects (ListObjectsV2)

List objects, optionally filtering by a prefix (which can include branch/commit ID).

```python
# Assuming s3_client, repo_name_boto, and branch_name_boto are defined from previous examples.
prefix_to_list_boto = f"{branch_name_boto}/uploads/" 

try:
    print(f"\nListing objects in '{repo_name_boto}' with prefix '{prefix_to_list_boto}':")
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=repo_name_boto, Prefix=prefix_to_list_boto)

    found_objects = False
    for page in page_iterator:
        if 'Contents' in page:
            found_objects = True
            for obj_data in page['Contents']: 
                print(f"- Key: {obj_data['Key']}, Size: {obj_data['Size']}, LastModified: {obj_data['LastModified']}")
    
    if not found_objects:
        print(f"No objects found with prefix '{prefix_to_list_boto}'.")

except ClientError as e:
    print(f"Boto3 ClientError during list_objects: {e.response.get('Error', {}).get('Code')} - {e}")
except Exception as e:
    print(f"An unexpected error occurred during list_objects: {e}")

# To list objects for a specific commit ID:
# commit_id_example = "your_actual_commit_id" 
# prefix_for_commit_example = f"{commit_id_example}/uploads/"
# ... then use prefix_for_commit_example in paginator.paginate(...) ...
```

### Downloading an Object (GetObject)

Download an object from lakeFS.

```python
# Assuming s3_client, repo_name_boto are defined.
key_to_download = f"{branch_name_boto}/uploads/sample_via_boto.txt" # Path from upload example
local_path_for_download = "downloaded_boto_file.txt" 

try:
    print(f"\nDownloading '{key_to_download}' from '{repo_name_boto}' to '{local_path_for_download}'...")
    response = s3_client.get_object(Bucket=repo_name_boto, Key=key_to_download)
    
    with open(local_path_for_download, 'wb') as f_download: 
        f_download.write(response['Body'].read())
    
    print("Download successful. Content:")
    with open(local_path_for_download, 'r') as f_content: 
        print(f_content.read())
except ClientError as e:
    if e.response.get('Error', {}).get('Code') == 'NoSuchKey':
        print(f"Error downloading: Object '{key_to_download}' not found in bucket '{repo_name_boto}'.")
    else:
        print(f"Boto3 ClientError during download: {e.response.get('Error', {}).get('Code')} - {e}")
except Exception as e:
    print(f"An unexpected error occurred during download: {e}")
finally:
    if os.path.exists(local_path_for_download):
        os.remove(local_path_for_download)
```

### Getting Object Metadata (HeadObject)

Retrieve metadata without downloading the object's content.

```python
# Assuming s3_client, repo_name_boto are defined.
key_for_metadata = f"{branch_name_boto}/uploads/sample_via_boto.txt" # Path from upload example

try:
    print(f"\nGetting metadata for '{key_for_metadata}' from '{repo_name_boto}'...")
    metadata_resp = s3_client.head_object(Bucket=repo_name_boto, Key=key_for_metadata) 
    
    print("Object metadata:")
    filtered_metadata = {k: v for k, v in metadata_resp.items() if k != 'ResponseMetadata'}
    for key_item, value_item in filtered_metadata.items(): 
        print(f"- {key_item}: {value_item}")
    if 'ETag' in metadata_resp: 
        print(f"ETag (Checksum): {metadata_resp['ETag'].strip('"')}") 
        
except ClientError as e:
    error_code = e.response.get('Error', {}).get('Code')
    if error_code == 'NoSuchKey' or error_code == '404': # Handle both common forms for "not found"
        print(f"Error getting metadata: Object '{key_for_metadata}' not found (Code: {error_code}).")
    else:
        print(f"Boto3 ClientError for head_object: {error_code} - {e}")
except Exception as e:
    print(f"An unexpected error occurred for head_object: {e}")
```

### Error Handling Notes

Boto3 operations typically raise `botocore.exceptions.ClientError` for API-related issues (like `NoSuchKey`, `AccessDenied`, etc.). This exception object contains detailed error information in `e.response['Error']`. It's recommended to catch `ClientError` specifically. Non-API client-side issues might raise other `botocore.exceptions` or standard Python exceptions.

```python
from botocore.exceptions import ClientError, NoCredentialsError

try:
    # ... your Boto3 operation ...
    # Example: s3_client.list_buckets() 
    print("Placeholder: Replace with an actual Boto3 operation for error handling demo.")
    pass 
except NoCredentialsError:
    print("Boto3 error: AWS credentials not found. Configure your credentials.")
except ClientError as e:
    error_code = e.response.get("Error", {}).get("Code")
    error_message = e.response.get("Error", {}).get("Message")
    print(f"Boto3 API Error! Code: {error_code}, Message: {error_message}")
except Exception as e: # Catch other general Python errors
    print(f"A general Python error occurred: {e}")
```

## Best Practices and Performance

For guidance on optimizing your use of Python with lakeFS, including Boto3 interactions, choosing the right tools for different scenarios, and general performance considerations, please refer to our comprehensive guide:
[Python SDKs: Best Practices & Performance](./best_practices.md)

---
