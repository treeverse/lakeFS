---
title: Objects and I/O Operations
description: Object management and streaming I/O with the High-Level Python SDK
sdk_types: ["high-level"]
difficulty: "intermediate"
use_cases: ["object-storage", "file-operations", "streaming", "large-files"]
topics: ["objects", "io", "streaming", "upload", "download"]
audience: ["data-engineers", "developers", "python-developers"]
last_updated: "2024-01-15"
---

# Objects and I/O Operations

Learn how to efficiently manage objects and perform I/O operations using the High-Level Python SDK. Objects in lakeFS represent files and data with full versioning capabilities and streaming I/O support.

## Object Concepts

### Object Types
- **StoredObject**: Read-only objects from references (commits, tags, branches)
- **WriteableObject**: Read-write objects from branches that support uploads and modifications
- **ObjectReader**: File-like interface for reading object data
- **ObjectWriter**: File-like interface for writing object data

### Object Paths
Objects are identified by their path within a repository and reference:
- Paths use forward slashes (`/`) as separators
- Paths are relative to the repository root
- No leading slash required (e.g., `data/file.txt`, not `/data/file.txt`)

### Streaming I/O
The SDK provides file-like objects for efficient streaming of large datasets without loading everything into memory.

## Object Upload Operations

### Simple Upload

```python
import lakefs

branch = lakefs.repository("my-repo").branch("main")

# Upload string data
obj = branch.object("data/file.txt").upload(
    data="Hello, lakeFS!",
    content_type="text/plain"
)

print(f"Uploaded: {obj.path}")
print(f"Repository: {obj.repo}")
print(f"Reference: {obj.ref}")
```

**Expected Output:**
```
Uploaded: data/file.txt
Repository: my-repo
Reference: main
```

### Upload Binary Data

```python
# Upload binary data from file
with open("local-file.pdf", "rb") as f:
    obj = branch.object("documents/report.pdf").upload(
        data=f.read(),
        content_type="application/pdf",
        metadata={"author": "data-team", "version": "1.0"}
    )

# Upload binary data directly
image_data = b'\x89PNG\r\n\x1a\n...'  # PNG image bytes
obj = branch.object("images/logo.png").upload(
    data=image_data,
    content_type="image/png"
)
```

### Upload with Different Modes

```python
# Create new file (fail if exists)
try:
    obj = branch.object("data/exclusive.txt").upload(
        data="Exclusive content",
        mode="x"  # Exclusive creation
    )
    print("File created successfully")
except ObjectExistsException:
    print("File already exists")

# Overwrite existing file
obj = branch.object("data/overwrite.txt").upload(
    data="New content",
    mode="w"  # Create or overwrite
)

# Binary modes
obj = branch.object("data/binary.dat").upload(
    data=b"Binary data",
    mode="wb"  # Binary write
)
```

### Upload with Metadata

```python
# Upload with custom metadata
obj = branch.object("data/dataset.csv").upload(
    data="name,age,city\nAlice,30,NYC\nBob,25,LA",
    content_type="text/csv",
    metadata={
        "source": "user_survey",
        "created_by": "data_pipeline",
        "version": "2.1",
        "schema_version": "1.0",
        "record_count": "2"
    }
)

print("Upload complete with metadata")
```

## Streaming Upload Operations

### Basic Streaming Upload

```python
import csv
import json

# Stream CSV data
obj = branch.object("data/users.csv")

with obj.writer(mode='w', content_type="text/csv") as writer:
    csv_writer = csv.writer(writer)
    csv_writer.writerow(["ID", "Name", "Email", "Age"])
    
    # Write data row by row (memory efficient)
    for i in range(1000):
        csv_writer.writerow([i, f"User{i}", f"user{i}@example.com", 20 + (i % 50)])

print("Streamed 1000 records to CSV")
```

### Large File Streaming

```python
# Stream large file efficiently
def upload_large_file(branch, source_path, target_path):
    """Upload large file using streaming to minimize memory usage"""
    
    obj = branch.object(target_path)
    
    with open(source_path, 'rb') as source:
        with obj.writer(mode='wb') as writer:
            # Stream in chunks
            chunk_size = 64 * 1024  # 64KB chunks
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                writer.write(chunk)
    
    return obj

# Usage
large_obj = upload_large_file(
    branch, 
    "local-large-file.dat", 
    "data/large-dataset.dat"
)
```

### JSON Streaming

```python
import json

# Stream JSON data
obj = branch.object("data/config.json")

config_data = {
    "database": {
        "host": "localhost",
        "port": 5432,
        "name": "myapp"
    },
    "features": {
        "auth": True,
        "logging": True,
        "metrics": True
    }
}

with obj.writer(mode='w', content_type="application/json") as writer:
    json.dump(config_data, writer, indent=2)

print("JSON configuration uploaded")
```

## Object Download Operations

### Simple Download

```python
# Read as string
content = branch.object("data/file.txt").reader().read()
print(f"Content: {content}")

# Read as bytes
binary_content = branch.object("data/file.txt").reader(mode='rb').read()
print(f"Bytes: {len(binary_content)} bytes")
```

**Expected Output:**
```
Content: Hello, lakeFS!
Bytes: 14 bytes
```

### Streaming Download

```python
import csv

# Stream CSV data for processing
obj = branch.object("data/users.csv")
processed_count = 0

with obj.reader(mode='r') as reader:
    csv_reader = csv.DictReader(reader)
    
    for row in csv_reader:
        # Process each row without loading entire file
        if int(row['Age']) > 30:
            print(f"Senior user: {row['Name']}")
            processed_count += 1

print(f"Processed {processed_count} senior users")
```

### Binary Download

```python
# Download binary data
obj = branch.object("documents/report.pdf")

# Method 1: Direct read
with obj.reader(mode='rb') as reader:
    pdf_data = reader.read()
    with open("downloaded-report.pdf", "wb") as f:
        f.write(pdf_data)

# Method 2: Streaming download
with obj.reader(mode='rb') as reader:
    with open("streamed-report.pdf", "wb") as f:
        chunk_size = 64 * 1024
        while True:
            chunk = reader.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)

print("PDF downloaded successfully")
```

### Partial Reading and Seeking

```python
# Read specific portions of file
obj = branch.object("data/large-file.txt")

with obj.reader(mode='r') as reader:
    # Read first 100 characters
    header = reader.read(100)
    print(f"Header: {header}")
    
    # Seek to middle of file
    file_size = obj.stat().size_bytes
    reader.seek(file_size // 2)
    
    # Read from middle
    middle_content = reader.read(50)
    print(f"Middle: {middle_content}")
    
    # Seek to end and read backwards
    reader.seek(-100, os.SEEK_END)
    tail = reader.read()
    print(f"Tail: {tail}")
```

## Object Metadata and Properties

### Object Statistics

```python
obj = branch.object("data/dataset.csv")
stats = obj.stat()

print(f"Path: {stats.path}")
print(f"Size: {stats.size_bytes} bytes")
print(f"Content Type: {stats.content_type}")
print(f"Checksum: {stats.checksum}")
print(f"Modified Time: {stats.mtime}")
print(f"Physical Address: {stats.physical_address}")

# Access custom metadata
if stats.metadata:
    print("Custom Metadata:")
    for key, value in stats.metadata.items():
        print(f"  {key}: {value}")
```

**Expected Output:**
```
Path: data/dataset.csv
Size: 1024 bytes
Content Type: text/csv
Checksum: sha256:a1b2c3d4...
Modified Time: 1640995200
Physical Address: s3://bucket/path/to/object
Custom Metadata:
  source: user_survey
  version: 2.1
```

### Pre-signed URLs

```python
# Get object stats with pre-signed URL
stats = obj.stat(pre_sign=True)
print(f"Pre-signed URL: {stats.physical_address}")

# Use pre-signed URLs for direct access
with obj.reader(pre_sign=True) as reader:
    content = reader.read()
    print("Read using pre-signed URL")
```

### Object Existence Checking

```python
# Check if object exists
if branch.object("data/maybe-exists.txt").exists():
    print("Object exists")
    content = branch.object("data/maybe-exists.txt").reader().read()
else:
    print("Object not found, creating it...")
    branch.object("data/maybe-exists.txt").upload(data="Default content")

# Batch existence checking
paths_to_check = ["data/file1.txt", "data/file2.txt", "data/file3.txt"]
existing_objects = []

for path in paths_to_check:
    if branch.object(path).exists():
        existing_objects.append(path)

print(f"Existing objects: {existing_objects}")
```

## Object Listing and Discovery

### List All Objects

```python
# List all objects in branch
print("All objects:")
for obj_info in branch.objects():
    print(f"  {obj_info.path} ({obj_info.size_bytes} bytes)")
```

**Expected Output:**
```
All objects:
  data/users.csv (2048 bytes)
  data/config.json (512 bytes)
  documents/report.pdf (102400 bytes)
```

### Filtered Object Listing

```python
# List objects with prefix
print("Data files:")
for obj_info in branch.objects(prefix="data/"):
    print(f"  {obj_info.path}")

# List objects with pagination
print("First 10 objects:")
for obj_info in branch.objects(max_amount=10):
    print(f"  {obj_info.path}")

# List objects after specific path
print("Objects after 'data/m':")
for obj_info in branch.objects(after="data/m"):
    print(f"  {obj_info.path}")
```

### Directory-like Listing

```python
# List with delimiter for directory-like structure
print("Top-level directories and files:")
for item in branch.objects(delimiter="/"):
    if hasattr(item, 'path') and item.path.endswith('/'):
        print(f"  📁 {item.path}")
    else:
        print(f"  📄 {item.path}")

# List specific directory contents
print("Contents of data/ directory:")
for item in branch.objects(prefix="data/", delimiter="/"):
    if hasattr(item, 'path'):
        print(f"  {item.path}")
```

### Advanced Object Discovery

```python
def find_objects_by_pattern(branch, pattern, max_results=100):
    """Find objects matching a pattern"""
    import re
    
    matching_objects = []
    regex = re.compile(pattern)
    
    for obj_info in branch.objects(max_amount=max_results):
        if regex.search(obj_info.path):
            matching_objects.append(obj_info)
    
    return matching_objects

# Find all CSV files
csv_files = find_objects_by_pattern(branch, r'\.csv$')
print(f"Found {len(csv_files)} CSV files")

# Find all files in subdirectories
nested_files = find_objects_by_pattern(branch, r'/.+/')
print(f"Found {len(nested_files)} files in subdirectories")
```

## Object Operations

### Copying Objects

```python
# Copy within same branch
source = branch.object("data/original.txt")
target_obj = source.copy(
    destination_branch_id="main",
    destination_path="data/copy.txt"
)

print(f"Copied to: {target_obj.path}")

# Copy to different branch
feature_branch = repo.branch("feature-branch")
copied_obj = source.copy(
    destination_branch_id="feature-branch",
    destination_path="data/feature-copy.txt"
)

# Verify copy exists
if copied_obj.exists():
    print("Copy successful")
```

### Deleting Objects

```python
# Delete single object
obj = branch.object("data/temp-file.txt")
if obj.exists():
    obj.delete()
    print("Object deleted")

# Delete multiple objects efficiently
objects_to_delete = [
    "temp/file1.txt",
    "temp/file2.txt", 
    "temp/file3.txt"
]

# Use branch-level delete for efficiency
branch.delete_objects(objects_to_delete)
print(f"Deleted {len(objects_to_delete)} objects")
```

### Object Comparison

```python
def compare_objects(obj1, obj2):
    """Compare two objects by content"""
    stats1 = obj1.stat()
    stats2 = obj2.stat()
    
    # Quick comparison by checksum
    if stats1.checksum == stats2.checksum:
        return True
    
    # Detailed comparison by content
    with obj1.reader(mode='rb') as r1, obj2.reader(mode='rb') as r2:
        return r1.read() == r2.read()

# Usage
obj1 = branch.object("data/file1.txt")
obj2 = branch.object("data/file2.txt")

if compare_objects(obj1, obj2):
    print("Objects are identical")
else:
    print("Objects differ")
```

## Advanced I/O Patterns

### Concurrent Object Processing

```python
import concurrent.futures
import threading

def process_object(obj_info):
    """Process a single object"""
    obj = branch.object(obj_info.path)
    
    try:
        with obj.reader(mode='r') as reader:
            content = reader.read()
            # Process content here
            return f"Processed {obj_info.path}: {len(content)} chars"
    except Exception as e:
        return f"Error processing {obj_info.path}: {e}"

# Process objects concurrently
objects_to_process = list(branch.objects(prefix="data/", max_amount=10))

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    results = list(executor.map(process_object, objects_to_process))

for result in results:
    print(result)
```

### Object Transformation Pipeline

```python
def transform_csv_object(source_obj, target_obj, transform_func):
    """Transform CSV data from source to target object"""
    import csv
    
    with source_obj.reader(mode='r') as reader:
        with target_obj.writer(mode='w', content_type="text/csv") as writer:
            csv_reader = csv.DictReader(reader)
            
            # Get fieldnames and potentially modify them
            fieldnames = csv_reader.fieldnames
            csv_writer = csv.DictWriter(writer, fieldnames=fieldnames)
            csv_writer.writeheader()
            
            # Transform each row
            for row in csv_reader:
                transformed_row = transform_func(row)
                csv_writer.writerow(transformed_row)

# Example transformation
def uppercase_names(row):
    row['name'] = row['name'].upper()
    return row

# Apply transformation
source = branch.object("data/users.csv")
target = branch.object("data/users_transformed.csv")
transform_csv_object(source, target, uppercase_names)
```

### Memory-Efficient Large File Processing

```python
def process_large_file_in_chunks(obj, chunk_size=1024*1024):
    """Process large file in chunks to minimize memory usage"""
    
    total_size = obj.stat().size_bytes
    processed_bytes = 0
    
    with obj.reader(mode='rb') as reader:
        while processed_bytes < total_size:
            chunk = reader.read(chunk_size)
            if not chunk:
                break
            
            # Process chunk here
            processed_bytes += len(chunk)
            progress = (processed_bytes / total_size) * 100
            print(f"Progress: {progress:.1f}%")
    
    print("Processing complete")

# Usage
large_obj = branch.object("data/large-dataset.dat")
process_large_file_in_chunks(large_obj)
```

## Error Handling and Best Practices

### Comprehensive Error Handling

```python
from lakefs.exceptions import (
    ObjectNotFoundException,
    ObjectExistsException,
    PermissionException
)

def robust_object_operations(branch, path, data):
    try:
        # Try to upload object
        obj = branch.object(path).upload(
            data=data,
            mode="x"  # Exclusive creation
        )
        print(f"Object created: {path}")
        return obj
        
    except ObjectExistsException:
        print(f"Object {path} already exists")
        # Decide whether to overwrite or use existing
        return branch.object(path)
        
    except PermissionException:
        print(f"Permission denied for {path}")
        return None
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Usage
obj = robust_object_operations(branch, "data/test.txt", "test content")
```

### Best Practices

```python
def object_best_practices():
    """Demonstrate best practices for object operations"""
    
    branch = lakefs.repository("my-repo").branch("main")
    
    # 1. Always use context managers for I/O
    obj = branch.object("data/example.txt")
    
    with obj.writer(mode='w') as writer:
        writer.write("Content written safely")
    # Writer automatically closed and data committed
    
    # 2. Check existence before operations
    if obj.exists():
        with obj.reader(mode='r') as reader:
            content = reader.read()
    
    # 3. Use appropriate content types
    branch.object("data/config.json").upload(
        data='{"key": "value"}',
        content_type="application/json"
    )
    
    # 4. Add meaningful metadata
    branch.object("data/dataset.csv").upload(
        data="name,value\ntest,123",
        content_type="text/csv",
        metadata={
            "source": "data_pipeline",
            "version": "1.0",
            "created_by": "automated_process"
        }
    )
    
    # 5. Use streaming for large files
    large_obj = branch.object("data/large-file.txt")
    with large_obj.writer(mode='w') as writer:
        for i in range(10000):
            writer.write(f"Line {i}\n")
    
    print("Best practices demonstrated")

# Run best practices example
object_best_practices()
```

## Key Points

- **File-like interface**: Objects support standard Python I/O operations
- **Streaming support**: Efficient handling of large files without memory issues
- **Metadata support**: Custom metadata can be attached to objects
- **Pre-signed URLs**: Direct access to underlying storage when supported
- **Atomic operations**: Uploads are atomic - either complete or fail entirely
- **Content type detection**: Automatic content type detection with manual override
- **Path flexibility**: Use forward slashes for cross-platform compatibility

## See Also

- **[Repository Management](repositories.md)** - Creating and managing repositories
- **[Branch Operations](branches-and-commits.md)** - Version control operations
- **[Transactions](transactions.md)** - Atomic multi-object operations
- **[Import Operations](imports-and-exports.md)** - Bulk data operations
- **[Best Practices](../reference/best-practices.md)** - Production deployment guidance