---
title: Python - Data Operations
description: Perform batch object operations, deletion, and metadata management in lakeFS with Python
---

# Working with Objects & Data Operations

This guide covers object operations in lakeFS, including uploading, downloading, batch operations, and metadata management.

## Prerequisites

- lakeFS server running and accessible
- Python SDK installed: `pip install lakefs`
- A repository with a branch (or create one in the examples)
- Proper credentials configured

## Understanding Objects & Files

### What are Objects & Files?

Objects are the files stored in lakeFS. Upload, download, and manage them through branches:

```python
import lakefs

branch = lakefs.repository("my-repo").branch("main")

# Upload a file
branch.object("data/dataset.csv").upload(
    data=b"id,name\n1,Alice\n2,Bob"
)

# Read a file
with branch.object("data/dataset.csv").reader() as f:
    print(f.read())
```

Objects in lakeFS allow you to:

- **Store files** at any path with support for large files
- **Version files** across branches and commits
- **Manage metadata** about file checksums, sizes, and types
- **Track changes** across data versions using diffs
- **Organize data** with hierarchical paths and prefixes

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

### Delete Objects Matching a Pattern

Delete all objects matching a prefix:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Find and delete temporary files
prefix = "temp/"
objects_to_delete = []

for obj in branch.objects(prefix=prefix):
    objects_to_delete.append(obj.path)

if objects_to_delete:
    branch.delete_objects(objects_to_delete)
    print(f"Deleted {len(objects_to_delete)} temporary files")
else:
    print("No temporary files found")
```

### Delete with Confirmation

Delete objects with safety checks:

```python
import lakefs

def safe_delete_objects(repo_name, branch_name, paths):
    """Delete objects with confirmation"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)
    
    # Show what will be deleted
    print(f"Will delete {len(paths)} objects:")
    for path in paths[:5]:
        print(f"  - {path}")
    if len(paths) > 5:
        print(f"  ... and {len(paths) - 5} more")
    
    # Confirm deletion (in real code, get user input)
    confirm = True
    
    if confirm:
        try:
            branch.delete_objects(paths)
            print(f"Successfully deleted {len(paths)} objects")
            return True
        except Exception as e:
            print(f"Delete failed: {e}")
            return False
    else:
        print("Deletion cancelled")
        return False


# Usage:
paths = ["data/old1.csv", "data/old2.csv", "data/old3.csv"]
safe_delete_objects("my-repo", "main", paths)
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

### Filter Objects by Type

Filter objects by extension or pattern:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Find all CSV files
csv_files = [
    obj for obj in branch.objects()
    if hasattr(obj, 'path') and obj.path.endswith('.csv')
]

print(f"CSV files: {len(csv_files)}")
for obj in csv_files[:10]:
    print(f"  {obj.path}")

# Find large files
large_files = [
    obj for obj in branch.objects()
    if hasattr(obj, 'path') and obj.size_bytes > 1000000  # > 1MB
]

print(f"\nLarge files (>1MB): {len(large_files)}")
for obj in large_files:
    size_mb = obj.size_bytes / (1024 * 1024)
    print(f"  {obj.path} ({size_mb:.2f} MB)")
```

### Paginated Listing

List objects with pagination:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# List with limit (pagination)
page_size = 100
after = None

page_num = 1
total_objects = 0

while True:
    print(f"\nPage {page_num}:")
    
    page_objects = []
    for obj in branch.objects(max_amount=page_size, after=after):
        page_objects.append(obj)
    
    if not page_objects:
        break
    
    for obj in page_objects:
        print(f"  {obj.path}")
        after = obj.path
    
    total_objects += len(page_objects)
    page_num += 1

print(f"\nTotal objects: {total_objects}")
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

### Update Object Metadata

Update existing object metadata:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Update metadata for an object
# Note: This typically requires re-uploading with new metadata
obj = branch.object("data/important.csv")

# Read current data
with obj.reader(mode='rb') as f:
    current_data = f.read()

# Re-upload with updated metadata
obj.upload(
    data=current_data,
    metadata={
        "owner": "data-team",
        "sensitivity": "confidential",
        "version": "2.0",
        "last-updated": "2024-01-15"
    }
)

print("Metadata updated")
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

### Data Validation and Filtering

Validate and filter data before keeping:

```python
import lakefs
import json

def validate_and_archive_logs(repo_name, source_branch, dest_branch):
    """Validate logs and move valid ones to archive"""
    repo = lakefs.repository(repo_name)
    source = repo.branch(source_branch)
    dest = repo.branch(dest_branch)
    
    valid_logs = []
    invalid_logs = []
    
    for obj in source.objects(prefix="logs/"):
        try:
            # Read log file
            with obj.reader(mode='r') as f:
                content = f.read()
            
            # Validate (simple check: starts with timestamp)
            lines = content.strip().split('\n')
            if lines and lines[0].startswith('['):
                valid_logs.append(obj.path)
            else:
                invalid_logs.append(obj.path)
                
        except Exception as e:
            invalid_logs.append(obj.path)
    
    # Copy valid logs to archive
    for log_path in valid_logs:
        try:
            with source.object(log_path).reader(mode='rb') as f:
                data = f.read()
            dest.object(log_path).upload(data=data)
        except Exception as e:
            print(f"Error archiving {log_path}: {e}")
    
    # Delete invalid logs
    if invalid_logs:
        source.delete_objects(invalid_logs)
    
    return len(valid_logs), len(invalid_logs)


# Usage:
valid, invalid = validate_and_archive_logs("logs-repo", "incoming", "archive")
print(f"Validated logs: {valid} valid, {invalid} invalid")
```

### Data Organization

Reorganize data into better structure:

```python
import lakefs

def reorganize_data(repo_name, source_prefix, dest_prefix, group_size=100):
    """Reorganize flat data structure into grouped structure"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch("main")
    
    objects = list(branch.objects(prefix=source_prefix))
    
    reorganized = 0
    
    # Group objects by index
    for i, obj in enumerate(objects):
        group_num = (i // group_size) + 1
        
        # Read data
        with obj.reader(mode='rb') as f:
            data = f.read()
        
        # Write to new location
        new_path = f"{dest_prefix}/group_{group_num:03d}/{obj.path.split('/')[-1]}"
        branch.object(new_path).upload(data=data)
        
        reorganized += 1
        
        if reorganized % 10 == 0:
            print(f"Reorganized {reorganized} objects...")
    
    return reorganized


# Usage:
count = reorganize_data("data-repo", "raw/", "organized/", group_size=50)
print(f"Reorganized {count} objects")
```

## Advanced Patterns

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

### Deduplication

Remove duplicate objects:

```python
import lakefs
import hashlib

def deduplicate_objects(repo_name, branch_name, prefix=""):
    """Find and remove duplicate objects"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)
    
    seen_hashes = {}
    duplicates = []
    
    # Calculate hashes
    for obj in branch.objects(prefix=prefix):
        try:
            with obj.reader(mode='rb') as f:
                content = f.read()
            
            file_hash = hashlib.md5(content).hexdigest()
            
            if file_hash in seen_hashes:
                duplicates.append(obj.path)
            else:
                seen_hashes[file_hash] = obj.path
                
        except Exception as e:
            print(f"Error processing {obj.path}: {e}")
    
    # Delete duplicates
    if duplicates:
        branch.delete_objects(duplicates)
        print(f"Removed {len(duplicates)} duplicate files")
    
    return len(duplicates)


# Usage:
dup_count = deduplicate_objects("data-repo", "main", "data/")
```

## Data Pipeline Workflow

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

## Best Practices

- **Use batch operations**: Delete multiple objects at once instead of individually
- **Limit listing results**: Use `max_amount` to avoid loading too many objects
- **Handle large files**: Use streaming/chunked reads for large objects
- **Include metadata**: Add meaningful metadata to objects for tracking
- **Validate before deleting**: Double-check objects before batch deletion
- **Use prefixes**: Organize data with clear prefixes for easy access
- **Verify operations**: Check results of batch operations
- **Clean up regularly**: Remove temporary and old files to save storage
- **Monitor object sizes**: Watch for unexpectedly large objects
