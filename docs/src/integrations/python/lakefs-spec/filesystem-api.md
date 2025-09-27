---
title: Filesystem API Operations
description: Core filesystem operations using lakefs-spec
sdk_types: ["lakefs-spec"]
difficulty: "beginner"
use_cases: ["filesystem-operations", "file-management", "path-operations"]
topics: ["filesystem", "operations", "paths", "files"]
audience: ["data-scientists", "analysts", "python-developers"]
last_updated: "2024-01-15"
---

# Filesystem API Operations

Learn how to perform standard filesystem operations using lakefs-spec's filesystem interface.

## Basic Setup

### Initialize Filesystem
```python
from lakefs_spec import LakeFSFileSystem

# Auto-discover credentials from ~/.lakectl.yaml or environment
fs = LakeFSFileSystem()

# Or specify credentials explicitly
fs = LakeFSFileSystem(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)
```

### URI Format
lakefs-spec uses the `lakefs://` URI scheme:
```
lakefs://repository/branch/path/to/file
lakefs://my-repo/main/data/file.txt
lakefs://my-repo/feature-branch/datasets/data.parquet
```

## File Operations

### Writing Files

#### Write Text Files
```python
from pathlib import Path

# Write string content directly
fs.write_text("lakefs://my-repo/main/data/sample.txt", "Hello, lakeFS!")

# Write from local file
local_file = Path("local_data.txt")
local_file.write_text("Local content")
fs.put_file("local_data.txt", "lakefs://my-repo/main/data/uploaded.txt")
```

#### Write Binary Files
```python
# Write binary data
binary_data = b"Binary content here"
fs.write_bytes("lakefs://my-repo/main/data/binary.dat", binary_data)

# Upload local binary file
fs.put_file("image.jpg", "lakefs://my-repo/main/images/uploaded.jpg")
```

#### Streaming Write
```python
# Write large files with streaming
with fs.open("lakefs://my-repo/main/data/large_file.txt", "w") as f:
    for i in range(1000):
        f.write(f"Line {i}\n")
```

### Reading Files

#### Read Text Files
```python
# Read entire file as string
content = fs.read_text("lakefs://my-repo/main/data/sample.txt")
print(content)  # "Hello, lakeFS!"

# Download to local file
fs.get_file("lakefs://my-repo/main/data/sample.txt", "downloaded.txt")
```

#### Read Binary Files
```python
# Read binary data
binary_content = fs.read_bytes("lakefs://my-repo/main/data/binary.dat")

# Download binary file
fs.get_file("lakefs://my-repo/main/images/uploaded.jpg", "downloaded.jpg")
```

#### Streaming Read
```python
# Read large files with streaming
with fs.open("lakefs://my-repo/main/data/large_file.txt", "r") as f:
    for line in f:
        print(line.strip())
```

### File Information

#### Check File Existence
```python
# Check if file exists
if fs.exists("lakefs://my-repo/main/data/sample.txt"):
    print("File exists")
else:
    print("File not found")
```

#### Get File Information
```python
# Get file info/stats
info = fs.info("lakefs://my-repo/main/data/sample.txt")
print(f"Size: {info['size']} bytes")
print(f"Type: {info['type']}")
print(f"Modified: {info['mtime']}")

# Get detailed stats
stats = fs.stat("lakefs://my-repo/main/data/sample.txt")
print(f"Checksum: {stats.get('checksum')}")
print(f"Content-Type: {stats.get('content_type')}")
```

## Directory Operations

### Listing Contents

#### List Files and Directories
```python
# List all files in a directory
files = fs.ls("lakefs://my-repo/main/data/")
for file_path in files:
    print(file_path)

# List with details
files_detailed = fs.ls("lakefs://my-repo/main/data/", detail=True)
for file_info in files_detailed:
    print(f"{file_info['name']} - {file_info['size']} bytes")
```

#### Recursive Listing
```python
# List all files recursively
all_files = fs.find("lakefs://my-repo/main/")
for file_path in all_files:
    print(file_path)

# Find specific file types
csv_files = fs.glob("lakefs://my-repo/main/**/*.csv")
for csv_file in csv_files:
    print(csv_file)
```

#### Directory Tree
```python
# Walk directory tree
for root, dirs, files in fs.walk("lakefs://my-repo/main/data/"):
    print(f"Directory: {root}")
    for file in files:
        print(f"  File: {file}")
```

### Creating Directories
```python
# Create directory (implicit with file creation)
fs.makedirs("lakefs://my-repo/main/new_directory/", exist_ok=True)

# Directories are created automatically when writing files
fs.write_text("lakefs://my-repo/main/new_dir/file.txt", "Content")
```

## File Management

### Copying Files
```python
# Copy within same repository
fs.copy(
    "lakefs://my-repo/main/data/source.txt",
    "lakefs://my-repo/main/data/copy.txt"
)

# Copy between branches
fs.copy(
    "lakefs://my-repo/main/data/file.txt",
    "lakefs://my-repo/feature-branch/data/file.txt"
)

# Copy multiple files
source_files = fs.glob("lakefs://my-repo/main/data/*.txt")
for source in source_files:
    target = source.replace("/main/", "/backup/")
    fs.copy(source, target)
```

### Moving/Renaming Files
```python
# Move/rename file
fs.move(
    "lakefs://my-repo/main/data/old_name.txt",
    "lakefs://my-repo/main/data/new_name.txt"
)

# Move to different directory
fs.move(
    "lakefs://my-repo/main/temp/file.txt",
    "lakefs://my-repo/main/permanent/file.txt"
)
```

### Deleting Files
```python
# Delete single file
fs.rm("lakefs://my-repo/main/data/unwanted.txt")

# Delete multiple files
files_to_delete = [
    "lakefs://my-repo/main/temp/file1.txt",
    "lakefs://my-repo/main/temp/file2.txt"
]
fs.rm(files_to_delete)

# Delete directory and all contents
fs.rm("lakefs://my-repo/main/temp/", recursive=True)
```

## Advanced Operations

### Batch Operations
```python
def batch_upload(fs, local_dir, remote_base):
    """Upload multiple files efficiently"""
    from pathlib import Path
    
    local_path = Path(local_dir)
    uploaded_files = []
    
    for local_file in local_path.rglob("*"):
        if local_file.is_file():
            # Calculate relative path
            rel_path = local_file.relative_to(local_path)
            remote_path = f"{remote_base}/{rel_path}"
            
            # Upload file
            fs.put_file(str(local_file), remote_path)
            uploaded_files.append(remote_path)
    
    return uploaded_files

# Usage
uploaded = batch_upload(fs, "local_data/", "lakefs://my-repo/main/uploaded/")
print(f"Uploaded {len(uploaded)} files")
```

### Working with Metadata
```python
# Files with custom metadata are handled through the underlying lakeFS API
# lakefs-spec focuses on filesystem operations

# Get file info including lakeFS-specific metadata
info = fs.info("lakefs://my-repo/main/data/file.txt")
if 'lakefs_metadata' in info:
    print(f"lakeFS metadata: {info['lakefs_metadata']}")
```

### Error Handling
```python
from fsspec.exceptions import FileNotFoundError

def safe_file_operation(fs, path):
    """Safely perform file operations with error handling"""
    try:
        if fs.exists(path):
            content = fs.read_text(path)
            return content
        else:
            print(f"File not found: {path}")
            return None
    except FileNotFoundError:
        print(f"File not found: {path}")
        return None
    except PermissionError:
        print(f"Permission denied: {path}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Usage
content = safe_file_operation(fs, "lakefs://my-repo/main/data/file.txt")
```

## Performance Tips

### Efficient File Operations
```python
# Use batch operations when possible
files_to_upload = ["file1.txt", "file2.txt", "file3.txt"]
remote_paths = [f"lakefs://my-repo/main/data/{f}" for f in files_to_upload]

# Upload in batch
for local, remote in zip(files_to_upload, remote_paths):
    fs.put_file(local, remote)

# Use streaming for large files
def stream_large_file(fs, local_path, remote_path):
    with open(local_path, 'rb') as local_f:
        with fs.open(remote_path, 'wb') as remote_f:
            chunk_size = 8192  # 8KB chunks
            while True:
                chunk = local_f.read(chunk_size)
                if not chunk:
                    break
                remote_f.write(chunk)
```

### Caching and Connection Management
```python
# lakefs-spec handles connection pooling automatically
# For better performance with many operations, reuse the filesystem instance

class LakeFSManager:
    def __init__(self):
        self.fs = LakeFSFileSystem()
    
    def upload_dataset(self, local_files, remote_base):
        """Upload multiple files using the same filesystem instance"""
        for local_file in local_files:
            remote_path = f"{remote_base}/{Path(local_file).name}"
            self.fs.put_file(local_file, remote_path)

# Usage
manager = LakeFSManager()
manager.upload_dataset(["data1.csv", "data2.csv"], "lakefs://my-repo/main/datasets/")
```

## Next Steps

- Learn about [data science integrations](integrations.md)
- Explore [transaction patterns](transactions.md)
- Check the [lakefs-spec documentation](https://lakefs-spec.org/)