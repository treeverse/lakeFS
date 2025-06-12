---
title: High-Level Python SDK (lakefs)
description: Learn how to use the High-Level Python SDK to interact with lakeFS for versioning, object operations, and more.
parent: Python Integration
grand_parent: Integrations
---

# High-Level Python SDK (`lakefs`)

{% raw %}{% include toc.html %}{% endraw %}

## Overview

The High-Level Python SDK (`lakefs`) is the recommended client library for most Python interactions with lakeFS. It provides an intuitive, Pythonic interface for common lakeFS operations, including repository and branch management, object I/O, commits, merges, and atomic transactions.

{: .note }
> This SDK is actively developed and is distinct from the [legacy `lakefs-client`](https://pypi.org/project/lakefs-client/) which is now [deprecated](../../posts/deprecate-py-legacy.html). If you are migrating, please check for any necessary code updates.

### Key Features

*   Simplified object operations (read, write, upload, download) with file-like semantics.
*   Easy management of repositories, branches, and tags.
*   Support for atomic commits and merges.
*   Transactional capabilities to perform a series of operations as a single atomic unit.
*   Built-in authentication handling, with environment-based credential discovery.

### Installation

Install the Python client using pip:

```shell
pip install lakefs
```

### Initializing the Client

The High-Level SDK will attempt to automatically collect authentication parameters from your environment (e.g., from a configured `lakectl.yaml` file or environment variables `LAKEFS_ACCESS_KEY_ID`, `LAKEFS_SECRET_ACCESS_KEY`, `LAKEFS_SERVER_ENDPOINT`).

If `lakectl` is configured, you often don't need to explicitly instantiate a lakeFS client for many operations.

However, you can explicitly create a `Client` instance if needed:

```python
from lakefs.client import Client

# Example: Explicit client configuration
clt = Client(
    host="http://your-lakefs-endpoint:8000",  # Replace with your lakeFS endpoint
    username="YOUR_ACCESS_KEY_ID",
    password="YOUR_SECRET_ACCESS_KEY"
)
```

{: .note }
> For enterprise users, see [Login with Python using AWS role](../../security/external-principals-aws.md#login-with-python) for alternative authentication methods.

#### Advanced Client Configuration

**TLS/SSL Verification:**

To use TLS with a custom CA certificate bundle:
```python
clt = Client(
    host="https://your-lakefs-endpoint:8000",
    username="YOUR_ACCESS_KEY_ID",
    password="YOUR_SECRET_ACCESS_KEY",
    ssl_ca_cert="path/to/your_concatenated_ca_certificates.pem"
)
```

To disable SSL verification (e.g., for testing with self-signed certificates):
```python
clt = Client(
    host="https://your-lakefs-endpoint:8000",
    username="YOUR_ACCESS_KEY_ID",
    password="YOUR_SECRET_ACCESS_KEY",
    verify_ssl=False
)
```
{: .warning }
> Disabling SSL verification (`verify_ssl=False`) can expose you to security risks like man-in-the-middle attacks. **Never use this in a production environment.**

**Proxy Configuration:**

To enable communication via a proxy server:
```python
clt = Client(
    host="http://your-lakefs-endpoint:8000",
    username="YOUR_ACCESS_KEY_ID",
    password="YOUR_SECRET_ACCESS_KEY",
    proxy="<proxy_server_url>" # Replace with your proxy server URL
)
```

### API Reference

For the complete API reference, detailed explanations of all classes, methods, and parameters, please visit the official **High-Level Python SDK Documentation**:
[https://pydocs-lakefs.lakefs.io](https://pydocs-lakefs.lakefs.io)

## Tutorials / User Guides

This section provides step-by-step examples for common tasks using the High-Level SDK. The examples assume you have configured your client as shown in the "Initializing the Client" section. Many examples use `lakefs.repository("example-repo")` which assumes a repository named `example-repo` exists and your client is auto-configured; adapt names and client instantiation as needed.

### Repository Management

#### Creating a Repository
```python
import lakefs

# Assumes client is auto-configured from environment
try:
    # Replace "s3://your-storage-bucket/repos/example-repo/" with your actual storage namespace
    repo = lakefs.repository("example-repo").create(storage_namespace="s3://your-storage-bucket/repos/example-repo/") 
    print(f"Repository created: {repo.id}")
except lakefs.exceptions.ConflictException:
    print("Repository 'example-repo' already exists.")
    repo = lakefs.repository("example-repo") # Get a reference to the existing repository
except lakefs.exceptions.LakeFSException as e:
    print(f"Error creating repository: {e}")


# Example with an explicit client:
# clt = Client(...) # Initialize your client as shown above
# Replace "s3://your-storage-bucket/repos/example-repo-explicit/" with your actual storage namespace
# repo_explicit = lakefs.Repository("example-repo-explicit", client=clt).create(storage_namespace="s3://your-storage-bucket/repos/example-repo-explicit/") 
# print(repo_explicit)
```

#### Listing Repositories
```python
import lakefs

try:
    print("Listing repositories:")
    for r_item in lakefs.repositories(): # Renamed variable to avoid potential conflicts
        print(f"- {r_item.id} (Default Branch: {r_item.default_branch}, Storage: {r_item.storage_namespace})")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error listing repositories: {e}")
```

### Branch and Reference Management

#### Creating a Branch
```python
import lakefs

repo_name = "example-repo" # Assumes repo exists
new_branch_name = "experiment-branch" # Renamed for clarity
source_branch_name = "main"

try:
    repo = lakefs.repository(repo_name)
    experiment_branch = repo.branch(new_branch_name).create(source_reference=source_branch_name)
    print(f"Branch '{experiment_branch.id}' created from '{source_branch_name}'. Commit ID: {experiment_branch.get_commit().id}")
except lakefs.exceptions.NotFoundException:
    print(f"Source branch '{source_branch_name}' not found in repo '{repo_name}'. Please ensure it exists and has commits.")
except lakefs.exceptions.ConflictException:
    print(f"Branch '{new_branch_name}' already exists in repo '{repo_name}'.")
    # experiment_branch = repo.branch(new_branch_name) # If you want to use the existing branch
except lakefs.exceptions.LakeFSException as e:
    print(f"Error creating branch '{new_branch_name}': {e}")
```

#### Listing Branches
```python
import lakefs

repo_name = "example-repo" # Assumes repo exists
try:
    repo = lakefs.repository(repo_name)
    print(f"Branches in '{repo.id}':")
    for branch_item in repo.branches(): 
        print(f"- {branch_item.id}")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error listing branches for repo '{repo_name}': {e}")

```

### Object Operations (I/O)

The SDK provides intuitive I/O semantics, allowing you to work with lakeFS objects much like local files. These examples assume you have an `experiment-branch` in your `example-repo`.

#### Uploading Data

**Simple upload (string or bytes):**
```python
import lakefs

repo_name = "example-repo"
branch_name = "experiment-branch"
object_path = "data/sample.txt"

try:
    repo = lakefs.repository(repo_name)
    branch_obj = repo.branch(branch_name) 
    obj_uploaded = branch_obj.object(object_path).upload( # Renamed variable
        data="This is my sample object data in lakeFS!",
        content_type="text/plain"
    )
    print(f"Uploaded object: {obj_uploaded.path}, Size: {obj_uploaded.stat().size_bytes} bytes")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error uploading object to '{repo_name}/{branch_name}/{object_path}': {e}")
```

**Streaming upload (using a writer context):**
This is useful for larger files or when generating data on the fly.
```python
import lakefs
import csv

repo_name = "example-repo"
branch_name = "experiment-branch"
csv_object_path = "data/report.csv" 

sample_data = [
    ["ID", "Name", "Value"],
    [1, "Alice", 100],
    [2, "Bob", 200],
    [3, "Carol", 150],
]

try:
    repo = lakefs.repository(repo_name)
    branch_obj = repo.branch(branch_name)
    csv_obj_writer = branch_obj.object(csv_object_path) # Renamed variable

    with csv_obj_writer.writer(mode='w', pre_sign=True, content_type="text/csv") as fd:
        csv_writer = csv.writer(fd) # Renamed variable
        for data_row in sample_data: # Renamed variable
            csv_writer.writerow(data_row)
    print(f"Successfully wrote '{csv_obj_writer.path}' to branch '{branch_obj.id}'. ETag: {csv_obj_writer.stat().checksum}")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error writing CSV object to '{repo_name}/{branch_name}/{csv_object_path}': {e}")
```
On exiting the `with` context, the object is uploaded to lakeFS.

#### Reading Data

**Simple read:**
```python
import lakefs

repo_name = "example-repo"
branch_name = "experiment-branch" # Or main, or a commit ID
object_to_read_path = "data/sample.txt" 

try:
    repo = lakefs.repository(repo_name)
    branch_obj = repo.branch(branch_name)
    obj_to_read = branch_obj.object(object_to_read_path)

    if obj_to_read.exists():
        content_data = obj_to_read.reader(mode='r').read() # Renamed variable
        print(f"Content of '{obj_to_read.path}':\n{content_data}")
    else:
        print(f"Object '{obj_to_read.path}' not found on branch '{branch_obj.id}'.")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error reading object '{repo_name}/{branch_name}/{object_to_read_path}': {e}")
```

**Streaming read (e.g., for CSV):**
```python
import lakefs
import csv

repo_name = "example-repo"
branch_name = "experiment-branch" # Or main, or a commit ID
csv_to_read_path = "data/report.csv" 

try:
    repo = lakefs.repository(repo_name)
    branch_obj = repo.branch(branch_name)
    csv_obj_to_read = branch_obj.object(csv_to_read_path)

    if csv_obj_to_read.exists():
        with csv_obj_to_read.reader(mode='r') as fd:
            csv_reader = csv.reader(fd) # Renamed variable
            print(f"Reading CSV from '{csv_obj_to_read.path}':")
            for row_data in csv_reader: 
                print(row_data)
    else:
        print(f"Object '{csv_obj_to_read.path}' not found on branch '{branch_obj.id}'.")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error reading CSV object '{repo_name}/{branch_name}/{csv_to_read_path}': {e}")
```

### Versioning Operations

These examples assume an `experiment-branch` that has changes compared to the `main` branch in `example-repo`.

#### Listing Uncommitted Changes
```python
import lakefs

repo_name = "example-repo"
branch_name = "experiment-branch"

try:
    repo = lakefs.repository(repo_name)
    branch_obj = repo.branch(branch_name)

    print(f"Uncommitted changes on branch '{branch_obj.id}':")
    any_changes_found = False # Renamed variable
    for diff_item in branch_obj.uncommitted(): 
        any_changes_found = True
        print(f"- Type: {diff_item.type}, Path: {diff_item.path}, Size: {diff_item.size_bytes if diff_item.size_bytes else 'N/A'}")

    if not any_changes_found:
        print("No uncommitted changes.")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error listing uncommitted changes for '{repo_name}/{branch_name}': {e}")
```

#### Committing Changes
```python
import lakefs

repo_name = "example-repo"
branch_name = "experiment-branch"

try:
    repo = lakefs.repository(repo_name)
    branch_obj = repo.branch(branch_name)
    
    # Ensure there are changes to commit for this example to be meaningful.
    # This typically follows upload operations like those shown above.
    
    commit_reference = branch_obj.commit( # Renamed variable
        message='Added sample data and report via SDK',
        metadata={'author': 'python_sdk_docs_example', 'version': '1.1'}
    )
    print(f"Committed to branch '{branch_obj.id}'. Commit ID: {commit_reference.id}")
    print(f"Commit metadata: {commit_reference.get_commit().metadata}")
except lakefs.exceptions.NoChangesException:
    print(f"No changes to commit on branch '{repo_name}/{branch_name}'.")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error committing changes to '{repo_name}/{branch_name}': {e}")
```

#### Diffing Branches or Commits
```python
import lakefs

repo_name = "example-repo"
base_ref_name = "main"
compare_ref_name = "experiment-branch" # Assumes this branch has new commits relative to main

try:
    repo = lakefs.repository(repo_name)
    base_ref_obj = repo.ref(base_ref_name) # Renamed variable, can be branch, commit, or tag

    print(f"Differences in '{compare_ref_name}' compared to '{base_ref_name}':")
    any_diff_found = False # Renamed variable
    # This shows changes IN compare_ref_name WHEN COMPARED AGAINST base_ref_obj
    for diff_item in base_ref_obj.diff(other_ref=compare_ref_name): 
        any_diff_found = True
        print(f"- Type: {diff_item.type}, Path: {diff_item.path}, Size: {diff_item.size_bytes if diff_item.size_bytes else 'N/A'}")

    if not any_diff_found:
        print(f"No differences found between '{base_ref_name}' and '{compare_ref_name}'.")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error diffing '{repo_name}/{base_ref_name}' and '{repo_name}/{compare_ref_name}': {e}")
```

#### Merging Branches
```python
import lakefs

repo_name = "example-repo"
source_branch_to_merge_name = "experiment-branch"
target_branch_name = "main"

try:
    repo = lakefs.repository(repo_name)
    source_branch_obj = repo.branch(source_branch_to_merge_name) 
    
    print(f"Attempting to merge '{source_branch_obj.id}' into '{target_branch_name}'...")
    merge_commit_ref_id = source_branch_obj.merge_into(target_branch_name, message="Merge experiment results via SDK") # Renamed
    print(f"Merge successful. Merge commit ID on '{target_branch_name}': {merge_commit_ref_id}")
except lakefs.exceptions.ConflictException as e:
    print(f"Merge conflict when merging '{source_branch_to_merge_name}' into '{target_branch_name}': {e}. Please resolve conflicts manually or using a merge strategy.")
except lakefs.exceptions.NoChangesException:
    print(f"No changes to merge from '{source_branch_to_merge_name}' into '{target_branch_name}'.")
except lakefs.exceptions.LakeFSException as e:
    print(f"Error during merge from '{source_branch_to_merge_name}' into '{target_branch_name}': {e}")
```

### Importing Data into lakeFS

The SDK's `ImportManager` simplifies importing data from an external object store (like S3) into lakeFS.

```python
import lakefs
import time

repo_name = "example-repo"
import_branch_name = "data-import-branch"
source_main_branch = "main" # Source for creating the import branch

try:
    repo = lakefs.repository(repo_name)
    import_branch_obj = repo.branch(import_branch_name) 

    if not import_branch_obj.exists():
        import_branch_obj.create(source_reference=source_main_branch)
        print(f"Created branch '{import_branch_obj.id}' for import.")

    # Configure the import
    # Replace with your actual S3 paths and desired lakeFS destinations:
    # - "s3://your-source-bucket/path/to/dataset1/"
    # - "s3://your-source-bucket/path/to/single_file.csv"
    importer = import_branch_obj.import_data(commit_message="Initial data import from S3 via SDK") \
        .prefix(source_uri="s3://your-source-bucket/path/to/dataset1/", destination="datasets/dataset1/") \
        .object(source_uri="s3://your-source-bucket/path/to/single_file.csv", destination="raw_files/single_file.csv")

    print("Starting data import...")
    # Asynchronous import
    import_status = importer.start() # Renamed variable
    print(f"Import started. Import ID: {import_status.id}")
    
    while not import_status.completed:
        time.sleep(5)
        current_import_status = importer.status() 
        print(f"Import progress: {current_import_status.ingested_objects} objects ingested. Completed: {current_import_status.completed}, Error: {current_import_status.error}")
        if current_import_status.error: 
            print(f"Import failed during processing: {current_import_status.error}")
            import_status = current_import_status # Update status to reflect error
            break
        if current_import_status.completed: 
            import_status = current_import_status # Update status to reflect completion
            break
        # No need to re-assign import_status here if just polling

    if import_status.error:
        print(f"Import failed: {import_status.error}")
    else:
        print(f"Import successful! Ingested {import_status.ingested_objects} objects.")
        if import_status.commit: 
             print(f"Commit ID for import: {import_status.commit.id}")

except lakefs.exceptions.LakeFSException as e:
    print(f"Error during data import setup or execution for repo '{repo_name}': {e}")
```

### Transactions

Transactions allow you to perform a sequence of operations on a branch as an atomic unit. If any operation fails, all changes are rolled back.

```python
import lakefs

repo_name = "example-repo"
tx_branch_name = "transaction-branch" 
source_main_branch_for_tx = "main"

try:
    repo = lakefs.repository(repo_name)
    branch_for_tx = repo.branch(tx_branch_name)

    if not branch_for_tx.exists():
        branch_for_tx.create(source_reference=source_main_branch_for_tx)
        print(f"Created branch '{branch_for_tx.id}' for transaction.")

    print(f"Starting transaction on branch '{branch_for_tx.id}'...")
    with branch_for_tx.transact(commit_message="Atomic update with multiple objects via SDK") as tx_context: # Renamed
        print(f"  Inside transaction (temporary branch: {tx_context.branch.id})")

        objects_to_delete = list(tx_context.objects(prefix="old_data/"))
        if objects_to_delete:
            print(f"  Deleting {len(objects_to_delete)} objects from 'old_data/'...")
            for obj_to_delete in objects_to_delete: 
                obj_to_delete.delete()
        else:
            print("  No objects found in 'old_data/' to delete.")

        new_obj_path = "new_data/atomic_file.txt"
        tx_context.object(new_obj_path).upload("This data was written atomically.")
        print(f"  Uploaded '{new_obj_path}'.")

    print("Transaction committed successfully to branch '{branch_for_tx.id}'.")
    
    # Verification steps
    if not list(branch_for_tx.objects(prefix="old_data/")):
        print("Objects under 'old_data/' successfully deleted from '{branch_for_tx.id}'.")
    if branch_for_tx.object("new_data/atomic_file.txt").exists():
        print("New object 'new_data/atomic_file.txt' successfully created on '{branch_for_tx.id}'.")

except lakefs.exceptions.TransactionFailedException as e:
    print(f"Transaction failed on branch '{tx_branch_name}': {e}")
except lakefs.exceptions.LakeFSException as e:
    print(f"An error occurred with transaction on branch '{tx_branch_name}': {e}")
```

### Error Handling

The `lakefs` SDK raises specific exceptions for various error conditions, all inheriting from `lakefs.exceptions.LakeFSException`. Common exceptions include:

*   `lakefs.exceptions.NotFoundException`: Resource (repository, branch, object, etc.) not found.
*   `lakefs.exceptions.ConflictException`: Resource creation conflict (e.g., repository already exists) or merge conflict.
*   `lakefs.exceptions.PermissionException`: Insufficient permissions for the operation.
*   `lakefs.exceptions.NoChangesException`: Commit or merge operation found no changes to apply.
*   `lakefs.exceptions.TransactionFailedException`: A transaction could not be completed.
*   `lakefs.exceptions.ImportException`: Error during data import.

It's good practice to wrap SDK calls in `try...except` blocks to handle these potential errors gracefully:

```python
import lakefs
from lakefs.exceptions import LakeFSException, NotFoundException, ConflictException

repo_to_check = "non-existent-repo" 
try:
    # Your lakeFS SDK operations here
    print(f"Attempting to stat repository: {repo_to_check}")
    repo_stat = lakefs.repository(repo_to_check).stat() 
    print(f"Stat for {repo_to_check}: {repo_stat}")
except NotFoundException:
    print(f"Caught: Repository or resource '{repo_to_check}' not found.")
except ConflictException as e: # Added variable e
    print(f"Caught: Operation resulted in a conflict for '{repo_to_check}': {e}")
except LakeFSException as e: # Catch any other lakeFS specific error
    print(f"An unexpected lakeFS error occurred with '{repo_to_check}': {e}")
except Exception as e: # Catch any other non-lakeFS error
    print(f"A general error occurred with '{repo_to_check}': {e}")
```

## Best Practices and Performance

For guidance on optimizing your use of the Python SDKs, choosing the right tools for different scenarios, and general performance considerations, please refer to our comprehensive guide:
[Python SDKs: Best Practices & Performance](./best_practices.md)
---
