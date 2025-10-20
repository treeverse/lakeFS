---
title: Python - lakefs-spec
description: Use lakefs-spec for file system operations and data science integration with lakeFS
---

# Using lakefs-spec for File System Operations

The [lakefs-spec](https://lakefs-spec.org/) project provides a filesystem-like API to lakeFS, built on top of [fsspec](https://github.com/fsspec/filesystem_spec). This integration is perfect for data science workflows, pandas integration, and scenarios where you want S3-like operations.

!!! note
    lakefs-spec is a third-party package maintained by the lakeFS community. For issues and questions, refer to the [lakefs-spec repository](https://github.com/aai-institute/lakefs-spec).

## When to Use

Use lakefs-spec when you:

- Need **file system-like operations** (open, read, write, delete)
- Work with **data science tools** (pandas, dask, polars)
- Want an **S3-compatible interface** without managing branches explicitly
- Need to **integrate with fsspec-compatible libraries**
- Prefer **familiar file operations** over versioning abstractions

For versioning-focused workflows (branches, tags, commits), use the **[High-Level SDK](./python.md)** instead.

## Installation

Install lakefs-spec using pip:

```shell
pip install lakefs-spec
```

Or upgrade to the latest version:

```shell
pip install --upgrade lakefs-spec
```

## Basic Setup

### Initializing the File System

```python
from lakefs_spec import LakeFSFileSystem

# Auto-discover credentials from ~/.lakectl.yaml
fs = LakeFSFileSystem()

# Or provide explicit credentials
fs = LakeFSFileSystem(
    host="http://localhost:8000",
    username="your-access-key",
    password="your-secret-key"
)
```

## File Operations

### Writing Files

```python
from pathlib import Path
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# Write text file
fs.pipe("my-repo/main/data/text.txt", b"Hello, lakeFS!")

# Write from local file
local_file = Path("local_data.csv")
local_file.write_text("id,name\n1,Alice\n2,Bob")
fs.put(str(local_file), "my-repo/main/data/imported.csv")

# Write using context manager
with fs.open("my-repo/main/data/output.txt", "w") as f:
    f.write("Data written to lakeFS")
```

### Reading Files

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# Read entire file
data = fs.cat("my-repo/main/data/text.txt")
print(data.decode())

# Read using context manager
with fs.open("my-repo/main/data/input.txt", "r") as f:
    content = f.read()
    print(content)

# Read in chunks (for large files)
with fs.open("my-repo/main/data/large_file.csv", "rb") as f:
    chunk = f.read(1024)
    while chunk:
        process(chunk)
        chunk = f.read(1024)
```

### Listing Files

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# List files at path
files = fs.ls("my-repo/main/data/")
for file in files:
    print(file)

# Find files with glob pattern
csv_files = fs.glob("my-repo/main/**/*.csv")
for csv in csv_files:
    print(csv)
```

### Deleting Files

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# Delete single file
fs.rm("my-repo/main/data/temp.txt")

# Delete directory recursively
fs.rm("my-repo/main/data/temp_dir", recursive=True)
```

## Pandas Integration

### Reading Data into Pandas

```python
import pandas as pd
from lakefs_spec import LakeFSFileSystem

# Read CSV directly from lakeFS
df = pd.read_csv("lakefs://my-repo/main/data/dataset.csv")
print(df.head())

# Read Parquet
df = pd.read_parquet("lakefs://my-repo/main/data/data.parquet")

# Read JSON
df = pd.read_json("lakefs://my-repo/main/data/data.json")
```

### Writing Data from Pandas

```python
import pandas as pd

# Create sample data
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Carol", "David", "Eve"],
    "value": [100, 200, 300, 400, 500]
})

# Write to lakeFS as CSV
df.to_csv("lakefs://my-repo/main/output/data.csv", index=False)

# Write as Parquet
df.to_parquet("lakefs://my-repo/main/output/data.parquet")

# Write as JSON
df.to_json("lakefs://my-repo/main/output/data.json")
```

### Data Science Workflow Example

```python
import pandas as pd
import numpy as np

# Read training data
train_df = pd.read_csv("lakefs://ml-repo/main/datasets/train.csv")
test_df = pd.read_csv("lakefs://ml-repo/main/datasets/test.csv")

# Process data
train_df["normalized_value"] = (train_df["value"] - train_df["value"].mean()) / train_df["value"].std()
test_df["normalized_value"] = (test_df["value"] - test_df["value"].mean()) / test_df["value"].std()

# Save processed data
train_df.to_parquet("lakefs://ml-repo/main/processed/train.parquet")
test_df.to_parquet("lakefs://ml-repo/main/processed/test.parquet")

print("Processing complete!")
```

## Transactions

### Atomic Operations with Transactions

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# Perform atomic operations
with fs.transaction("my-repo", "main") as tx:
    # All operations happen on ephemeral branch
    fs.pipe(
        f"my-repo/{tx.branch.id}/data/file1.txt",
        b"Content 1"
    )
    fs.pipe(
        f"my-repo/{tx.branch.id}/data/file2.txt",
        b"Content 2"
    )
    
    # Commit when done
    tx.commit(message="Add files atomically")
    print(f"Committed: {tx.branch.id}")
```

### Transaction with Error Handling

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

try:
    with fs.transaction("my-repo", "main") as tx:
        # Perform operations
        fs.pipe(f"my-repo/{tx.branch.id}/file.txt", b"data")
        
        # Validate
        stat = fs.stat(f"my-repo/{tx.branch.id}/file.txt")
        if stat["size"] < 100:
            raise ValueError("File too small")
        
        tx.commit(message="Validated and committed")
        
except Exception as e:
    print(f"Transaction failed: {e}")
    print("Changes rolled back automatically")
```

### Tagging After Transaction

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction("ml-repo", "main") as tx:
    # Train and save model
    fs.pipe(f"ml-repo/{tx.branch.id}/models/model.pkl", model_data)
    
    # Save metrics
    fs.pipe(f"ml-repo/{tx.branch.id}/metrics.json", metrics_data)
    
    # Commit
    tx.commit(message="Model v1.0")
    
    # Tag as release
    tx.tag("v1.0.0")
    print("Model released as v1.0.0")
```

## Real-World Examples

### ETL Pipeline

```python
import pandas as pd
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# Extract: Read from multiple sources
raw_files = fs.glob("my-repo/main/raw/*.csv")
dfs = [pd.read_csv(f"lakefs://{f}") for f in raw_files]
combined = pd.concat(dfs)

# Transform: Clean and process
combined = combined.dropna()
combined["timestamp"] = pd.to_datetime(combined["timestamp"])
combined["normalized"] = (combined["value"] - combined["value"].mean()) / combined["value"].std()

# Load: Write processed data
combined.to_parquet("lakefs://my-repo/main/processed/data.parquet")
print(f"ETL complete: {len(combined)} rows processed")
```

### Data Analysis and Reporting

```python
import pandas as pd
import json
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# Read data for analysis
df = pd.read_parquet("lakefs://analytics-repo/main/data/raw_data.parquet")

# Perform analysis
summary = {
    "total_records": len(df),
    "mean_value": float(df["value"].mean()),
    "median_value": float(df["value"].median()),
    "std_value": float(df["value"].std())
}

# Save report
report = json.dumps(summary, indent=2)
fs.pipe("lakefs://analytics-repo/main/reports/summary.json", report.encode())

print("Analysis report saved")
```

### Model Versioning

```python
import pickle
from datetime import datetime
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

def save_model_version(repo, model, version, metrics):
    """Save model with version and metrics"""
    timestamp = datetime.now().isoformat()
    
    with fs.transaction(repo, "main") as tx:
        branch_id = tx.branch.id
        
        # Save model
        model_bytes = pickle.dumps(model)
        fs.pipe(
            f"{repo}/{branch_id}/models/{version}/model.pkl",
            model_bytes
        )
        
        # Save metrics
        metrics_json = json.dumps({
            "version": version,
            "timestamp": timestamp,
            **metrics
        })
        fs.pipe(
            f"{repo}/{branch_id}/models/{version}/metrics.json",
            metrics_json.encode()
        )
        
        # Commit
        tx.commit(message=f"Model {version}")
        
        # Tag for reference
        tx.tag(f"model-{version}")
        print(f"Model {version} saved and tagged")


# Usage:
model = train_model(training_data)
save_model_version(
    "ml-repo",
    model,
    "v2.1.0",
    {"accuracy": 0.95, "f1": 0.94}
)
```

## Comparison with Other Options

| Feature | lakefs-spec | High-Level SDK | Boto |
|---------|------------|----------------|------|
| File System API | ✅ Yes | No | No |
| Pandas Integration | ✅ Yes | No | No |
| Versioning Operations | Limited | ✅ Full | No |
| S3 Compatibility | Partial | No | ✅ Full |
| Learning Curve | Gentle | Gentle | Gentle |
| Use Case | Data Science | Versioning | S3 Workflows |

## Best Practices

- **Use context managers** - Always use `with` for transactions to ensure proper rollback on errors
- **Handle large files** - Use chunked reading for files larger than available memory
- **Version your data** - Use branches or tags to track data versions with transformations
- **Error handling** - Wrap transactions in try-except to handle failures gracefully
- **Credentials** - Use environment variables or lakectl config for secure credential management

## Further Resources

- **[lakefs-spec Project](https://lakefs-spec.org/)** - Official project documentation
- **[fsspec Documentation](https://filesystem-spec.readthedocs.io/)** - Filesystem spec reference
- **[Data Science Integrations](https://lakefs-spec.org/latest/guides/integrations/)** - pandas, dask, polars examples
