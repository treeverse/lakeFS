---
title: Python - Data Operations
description: Perform batch object operations, deletion, and metadata management in lakeFS with Python
---

# Working with Objects & Data Operations

This guide covers object operations in lakeFS, including uploading, downloading, batch operations, and metadata management.

## Basic Object Operations

### Uploading Objects

Upload data to lakeFS:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Upload text data
branch.object("data/simple.txt").upload(
    data=b"Hello, lakeFS!"
)

# Upload with content type
branch.object("data/data.json").upload(
    data=b'{"key": "value"}',
    content_type="application/json"
)

# Upload larger data
csv_data = b"id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300"
branch.object("data/records.csv").upload(data=csv_data)

print("Objects uploaded successfully")
```

### Downloading Objects

Read object data from lakeFS:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Read as text
with branch.object("data/simple.txt").reader(mode='r') as f:
    content = f.read()
    print(f"Content: {content}")

# Read as binary
with branch.object("data/data.json").reader(mode='rb') as f:
    binary_content = f.read()
    print(f"Binary size: {len(binary_content)} bytes")

# Read CSV and process
import csv
import io

with branch.object("data/records.csv").reader(mode='r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(f"  {row['name']}: {row['value']}")
```

### Object Information & Metadata

Get object details and metadata:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

obj = branch.object("data/records.csv")

# Check if object exists
try:
    if obj.exists():
        print("Object exists")
except:
    print("Object not found")

# Get object statistics
stat = obj.stat()
print(f"Size: {stat.size_bytes} bytes")
print(f"Modified: {stat.mtime}")
print(f"Checksum: {stat.checksum}")
print(f"Content Type: {stat.content_type}")
print(f"Path: {stat.path}")
```

### Deleting Objects

Remove objects from lakeFS:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Delete a single object
obj = branch.object("data/temp_file.txt")
obj.delete()
print("Object deleted")

# Handle non-existent objects gracefully
try:
    obj.delete()
except Exception as e:
    print(f"Delete failed: {e}")
```

## Batch Operations

### Batch Delete Multiple Objects

Delete many objects efficiently:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Delete multiple objects by path
paths_to_delete = [
    "data/file1.csv",
    "data/file2.csv",
    "data/file3.csv",
    "logs/temp.log"
]

try:
    branch.delete_objects(paths_to_delete)
    print(f"Deleted {len(paths_to_delete)} objects")
except Exception as e:
    print(f"Batch delete failed: {e}")
```

## Listing and Filtering Objects

### List Objects by Prefix

List all objects under a path:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# List all objects in data/ folder
print("Objects in data/:")
for obj in branch.objects(prefix="data/"):
    print(f"  {obj.path} ({obj.size_bytes} bytes)")

# Count total objects
total_objects = 0
for _ in branch.objects(prefix="data/"):
    total_objects += 1
print(f"Total objects: {total_objects}")
```

### List with Delimiter (Folder View)

Use delimiter to see folder structure:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# List with folder delimiter
print("Folder structure (with /):")
for item in branch.objects(prefix="", delimiter="/"):
    if hasattr(item, 'path'):
        # It's a file
        print(f"  FILE: {item.path}")
    else:
        # It's a folder
        print(f"  FOLDER: {item.name}")
```

## Working with Object Metadata

### Set Custom Object Metadata

Attach custom metadata to objects:

```python
import lakefs
import json

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Create object with metadata
obj = branch.object("data/important.csv")
obj.upload(
    data=b"id,value\n1,100",
    metadata={
        "owner": "data-team",
        "sensitivity": "public",
        "version": "1.0"
    }
)

print("Object uploaded with metadata")
```

### Read Object Metadata

Retrieve object metadata:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

obj = branch.object("data/important.csv")
stat = obj.stat()

print(f"Object: {stat.path}")
print(f"Size: {stat.size_bytes}")
print(f"Metadata: {stat.metadata}")
```

## Real-World Workflows

### Data Cleanup

Remove old and temporary files:

```python
import lakefs
from datetime import datetime, timedelta

def cleanup_old_files(repo_name, branch_name, days_old=7):
    """Delete files older than specified days"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)

    cutoff_time = datetime.now().timestamp() - (days_old * 24 * 60 * 60)

    old_files = []

    for obj in branch.objects():
        if hasattr(obj, 'mtime') and obj.mtime < cutoff_time:
            old_files.append(obj.path)

    if old_files:
        print(f"Found {len(old_files)} files older than {days_old} days")
        branch.delete_objects(old_files)
        print(f"Deleted {len(old_files)} old files")
        return len(old_files)
    else:
        print("No old files to delete")
        return 0


# Usage:
deleted_count = cleanup_old_files("archive-repo", "main", days_old=30)
print(f"Cleanup complete: {deleted_count} files removed")
```

### Bulk Data Import

Import multiple files efficiently:

```python
import lakefs
import os

def bulk_import_files(repo_name, branch_name, local_dir, lakeFS_prefix):
    """Import all files from local directory"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)

    imported = 0
    errors = 0

    # Walk local directory
    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)

            # Calculate lakeFS path
            rel_path = os.path.relpath(local_path, local_dir)
            lakeFS_path = f"{lakeFS_prefix}/{rel_path}".replace("\\", "/")

            try:
                # Read and upload file
                with open(local_path, 'rb') as f:
                    data = f.read()

                branch.object(lakeFS_path).upload(data=data)
                print(f"  Imported: {lakeFS_path}")
                imported += 1

            except Exception as e:
                print(f"  Error importing {lakeFS_path}: {e}")
                errors += 1

    return imported, errors


# Usage (pseudo-code - adjust for your environment):
# imported, errors = bulk_import_files(
#     "my-repo",
#     "main",
#     "/local/data/directory",
#     "data/imports"
# )
# print(f"Imported: {imported}, Errors: {errors}")
```

### Stream Processing

Process large files efficiently:

```python
import lakefs
import io

def process_csv_stream(repo_name, branch_name, file_path, processor_func):
    """Process large CSV file line by line"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)

    processed = 0

    with branch.object(file_path).reader(mode='r') as f:
        for line in f:
            processor_func(line.strip())
            processed += 1

    return processed


# Usage:
def count_records(line):
    pass  # Do something with each line


count = process_csv_stream(
    "data-repo",
    "main",
    "data/large_file.csv",
    count_records
)
```

### Creating a Complete Data Pipeline

Implement an end-to-end pipeline with data operations, transactions, and merging:

```python
import lakefs

# Get repository and create experiment branch
repo = lakefs.repository("analytics-repo")
branch = repo.branch("processing-v2").create(source_reference="main")

try:
    # Upload raw data
    branch.object("raw/input.csv").upload(data=raw_data)

    # Perform transformations with transactions
    with branch.transact(commit_message="Process raw data") as tx:
        # Read and transform
        with tx.object("raw/input.csv").reader() as f:
            processed = transform(f.read())

        # Write processed data
        tx.object("processed/output.csv").upload(data=processed)

    # Review changes before merging
    changes = list(branch.uncommitted())
    print(f"Changes: {len(changes)} objects")

    # Merge to main if satisfied
    branch.merge_into(repo.branch("main"))

except Exception as e:
    print(f"Error in pipeline: {e}")
    branch.delete()  # Clean up on failure
```

This pattern ensures:

- Raw data is preserved in isolation
- Transformations are atomic (all-or-nothing)
- Changes are reviewable before integration
- Failed pipelines can be safely cleaned up

## Error Handling

### Handling Object Errors

```python
import lakefs
from lakefs.exceptions import NotFoundException, ObjectNotFoundException

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Object not found
try:
    obj = branch.object("non-existent.csv")
    obj.delete()
except (NotFoundException, ObjectNotFoundException):
    print("Object not found")

# Permission denied
try:
    obj = branch.object("data/file.csv")
    obj.upload(data=b"data")
except Exception as e:
    print(f"Upload failed: {e}")
```
