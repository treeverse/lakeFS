---
title: Data Imports and Exports
description: Import and export data using the High-Level Python SDK
sdk_types: ["high-level"]
difficulty: "intermediate"
use_cases: ["data-import", "bulk-operations", "data-migration", "etl"]
topics: ["import", "export", "bulk-data", "migration"]
audience: ["data-engineers", "developers", "python-developers"]
last_updated: "2024-01-15"
---

# Data Imports and Exports

Learn how to efficiently import existing data into lakeFS and export data from lakeFS repositories. The ImportManager provides powerful capabilities for bulk data operations with progress monitoring and error handling.

## Import Concepts

### ImportManager Overview
The ImportManager provides a fluent interface for importing data from external storage systems into lakeFS branches. It supports:
- **Multiple source types**: Individual objects and prefixes (directories)
- **Batch operations**: Import from multiple sources in a single operation
- **Asynchronous execution**: Non-blocking imports with progress monitoring
- **Atomic commits**: All imported data is committed together

### Import Sources
- **Object imports**: Import specific files by their full URI
- **Prefix imports**: Import entire directories or prefixes
- **Mixed imports**: Combine objects and prefixes in a single operation

### Import Process
1. Create ImportManager with commit message and metadata
2. Add sources (objects and/or prefixes)
3. Start import (synchronous or asynchronous)
4. Monitor progress and handle completion

## Basic Import Operations

### Simple Object Import

```python
import lakefs

branch = lakefs.repository("my-repo").branch("main")

# Import a single object
importer = branch.import_data(commit_message="Import configuration file")
importer.object(
    object_store_uri="s3://source-bucket/config/app.yaml",
    destination="config/app.yaml"
)

# Execute the import
result = importer.run()
print(f"Imported {result.ingested_objects} objects")
print(f"Commit ID: {result.commit.id}")
```

**Expected Output:**
```
Imported 1 objects
Commit ID: a1b2c3d4e5f6...
```

### Prefix Import

```python
# Import entire directory/prefix
importer = branch.import_data(commit_message="Import dataset")
importer.prefix(
    object_store_uri="s3://source-bucket/datasets/user-data/",
    destination="data/users/"
)

result = importer.run()
print(f"Imported {result.ingested_objects} objects from prefix")
```

### Multiple Source Import

```python
# Import from multiple sources in one operation
importer = branch.import_data(
    commit_message="Import application data and configs",
    metadata={
        "source": "production-backup",
        "date": "2024-01-15",
        "environment": "prod"
    }
)

# Add multiple sources
importer.prefix("s3://backup-bucket/data/", destination="data/") \
        .prefix("s3://backup-bucket/logs/", destination="logs/") \
        .object("s3://backup-bucket/config.json", destination="config/app.json") \
        .object("s3://backup-bucket/schema.sql", destination="db/schema.sql")

# Execute import
result = importer.run()
print(f"Multi-source import completed: {result.ingested_objects} objects")
```

**Expected Output:**
```
Multi-source import completed: 1247 objects
```

## Asynchronous Import Operations

### Non-blocking Import

```python
import time
from datetime import timedelta

# Start import without blocking
importer = branch.import_data(commit_message="Large dataset import")
importer.prefix("s3://large-dataset-bucket/", destination="data/")

# Start the import (non-blocking)
import_id = importer.start()
print(f"Import started with ID: {import_id}")

# Monitor progress
while True:
    status = importer.status()
    
    if status.completed:
        print(f"Import completed! Total objects: {status.ingested_objects}")
        print(f"Commit ID: {status.commit.id}")
        break
    
    if status.error:
        print(f"Import failed: {status.error.message}")
        break
    
    print(f"In progress... {status.ingested_objects} objects imported so far")
    time.sleep(10)  # Check every 10 seconds
```

**Expected Output:**
```
Import started with ID: import-123456
In progress... 150 objects imported so far
In progress... 300 objects imported so far
In progress... 450 objects imported so far
Import completed! Total objects: 523
Commit ID: b2c3d4e5f6a7...
```

### Import with Custom Polling

```python
# Import with custom polling interval
importer = branch.import_data(commit_message="Custom polling import")
importer.prefix("s3://source/data/", destination="imported/")

# Start and wait with custom interval
result = importer.run(poll_interval=timedelta(seconds=5))
print(f"Import completed with custom polling: {result.ingested_objects} objects")
```

### Waiting for Completion

```python
# Start import and wait separately
importer = branch.import_data(commit_message="Separate start/wait")
importer.prefix("s3://data-source/", destination="data/")

# Start import
import_id = importer.start()
print(f"Started import: {import_id}")

# Do other work here...
print("Performing other operations...")

# Wait for completion
result = importer.wait(poll_interval=timedelta(seconds=3))
print(f"Import finished: {result.ingested_objects} objects")
```

## Import Status and Monitoring

### Detailed Status Information

```python
# Start an import
importer = branch.import_data(commit_message="Status monitoring example")
importer.prefix("s3://source/large-dataset/", destination="data/")

import_id = importer.start()

# Get detailed status
status = importer.status()

print(f"Import ID: {importer.import_id}")
print(f"Completed: {status.completed}")
print(f"Objects ingested: {status.ingested_objects}")
print(f"Update time: {status.update_time}")
print(f"Metarange ID: {status.metarange_id}")

if status.error:
    print(f"Error: {status.error.message}")

if status.commit:
    print(f"Commit ID: {status.commit.id}")
    print(f"Commit message: {status.commit.message}")
```

### Progress Monitoring with Callbacks

```python
def monitor_import_progress(importer, callback_interval=5):
    """Monitor import progress with custom callback"""
    
    import_id = importer.start()
    start_time = time.time()
    
    while True:
        status = importer.status()
        elapsed = time.time() - start_time
        
        if status.completed:
            print(f"✅ Import completed in {elapsed:.1f}s")
            print(f"   Total objects: {status.ingested_objects}")
            return status
        
        if status.error:
            print(f"❌ Import failed after {elapsed:.1f}s: {status.error.message}")
            return status
        
        # Progress callback
        objects_per_second = status.ingested_objects / elapsed if elapsed > 0 else 0
        print(f"⏳ Progress: {status.ingested_objects} objects ({objects_per_second:.1f}/sec)")
        
        time.sleep(callback_interval)

# Usage
importer = branch.import_data(commit_message="Monitored import")
importer.prefix("s3://large-source/", destination="data/")

final_status = monitor_import_progress(importer)
```

## Import Configuration and Metadata

### Import with Rich Metadata

```python
# Import with comprehensive metadata
importer = branch.import_data(
    commit_message="Production data import - Q4 2024",
    metadata={
        "source_system": "production-db",
        "import_date": "2024-01-15",
        "data_version": "v2.1.0",
        "environment": "production",
        "imported_by": "data-pipeline",
        "validation_status": "passed",
        "record_count": "1000000",
        "size_gb": "15.7"
    }
)

importer.prefix("s3://prod-backup/q4-data/", destination="data/q4/")
result = importer.run()

# Verify metadata in commit
commit = result.commit
print("Import metadata:")
for key, value in commit.metadata.items():
    print(f"  {key}: {value}")
```

### Conditional Import Based on Existing Data

```python
def conditional_import(branch, source_uri, destination, force=False):
    """Import data only if destination doesn't exist or force is True"""
    
    # Check if destination already has data
    existing_objects = list(branch.objects(prefix=destination, max_amount=1))
    
    if existing_objects and not force:
        print(f"Destination {destination} already contains data. Use force=True to overwrite.")
        return None
    
    # Proceed with import
    importer = branch.import_data(
        commit_message=f"Import to {destination}",
        metadata={"overwrite": str(force)}
    )
    importer.prefix(source_uri, destination=destination)
    
    return importer.run()

# Usage
result = conditional_import(
    branch, 
    "s3://source/data/", 
    "imported/data/",
    force=False
)
```

## Error Handling and Recovery

### Comprehensive Error Handling

```python
from lakefs.exceptions import ImportManagerException, NotFoundException

def robust_import(branch, sources, commit_message):
    """Perform import with comprehensive error handling"""
    
    try:
        # Create importer
        importer = branch.import_data(commit_message=commit_message)
        
        # Add sources
        for source_type, uri, destination in sources:
            if source_type == "prefix":
                importer.prefix(uri, destination=destination)
            elif source_type == "object":
                importer.object(uri, destination=destination)
            else:
                raise ValueError(f"Unknown source type: {source_type}")
        
        # Start import
        import_id = importer.start()
        print(f"Import started: {import_id}")
        
        # Wait for completion with timeout
        max_wait_time = 3600  # 1 hour
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            status = importer.status()
            
            if status.completed:
                print(f"Import successful: {status.ingested_objects} objects")
                return status
            
            if status.error:
                print(f"Import error: {status.error.message}")
                return None
            
            time.sleep(10)
        
        # Timeout - cancel import
        print("Import timeout, cancelling...")
        importer.cancel()
        return None
        
    except ImportManagerException as e:
        print(f"Import manager error: {e}")
        return None
    except NotFoundException as e:
        print(f"Resource not found: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Usage
sources = [
    ("prefix", "s3://source/data/", "imported/data/"),
    ("object", "s3://source/config.yaml", "config/app.yaml")
]

result = robust_import(branch, sources, "Robust import example")
```

### Import Cancellation

```python
import threading

def cancellable_import(branch, source_uri, destination):
    """Import with cancellation capability"""
    
    importer = branch.import_data(commit_message="Cancellable import")
    importer.prefix(source_uri, destination=destination)
    
    # Start import
    import_id = importer.start()
    print(f"Import started: {import_id}")
    
    # Simulate user cancellation after 30 seconds
    def cancel_after_delay():
        time.sleep(30)
        try:
            importer.cancel()
            print("Import cancelled by user")
        except Exception as e:
            print(f"Cancellation failed: {e}")
    
    cancel_thread = threading.Thread(target=cancel_after_delay)
    cancel_thread.start()
    
    # Monitor import
    try:
        result = importer.wait()
        print(f"Import completed: {result.ingested_objects} objects")
        return result
    except ImportManagerException as e:
        print(f"Import was cancelled or failed: {e}")
        return None

# Usage
result = cancellable_import(branch, "s3://large-source/", "data/")
```

## Advanced Import Patterns

### Batch Import with Validation

```python
def validated_batch_import(branch, import_configs):
    """Import multiple datasets with validation"""
    
    results = []
    
    for config in import_configs:
        name = config['name']
        sources = config['sources']
        validation_func = config.get('validation')
        
        print(f"Starting import: {name}")
        
        # Create importer
        importer = branch.import_data(
            commit_message=f"Import {name}",
            metadata={"batch_import": "true", "dataset": name}
        )
        
        # Add sources
        for source in sources:
            if source['type'] == 'prefix':
                importer.prefix(source['uri'], destination=source['destination'])
            else:
                importer.object(source['uri'], destination=source['destination'])
        
        # Execute import
        try:
            result = importer.run()
            
            # Validate if validation function provided
            if validation_func:
                if validation_func(branch, result):
                    print(f"✅ {name}: Import and validation successful")
                else:
                    print(f"❌ {name}: Import successful but validation failed")
            else:
                print(f"✅ {name}: Import successful ({result.ingested_objects} objects)")
            
            results.append({"name": name, "result": result, "success": True})
            
        except Exception as e:
            print(f"❌ {name}: Import failed - {e}")
            results.append({"name": name, "error": str(e), "success": False})
    
    return results

# Example validation function
def validate_csv_import(branch, import_result):
    """Validate that CSV files were imported correctly"""
    csv_objects = list(branch.objects(prefix="data/", max_amount=100))
    csv_files = [obj for obj in csv_objects if obj.path.endswith('.csv')]
    return len(csv_files) > 0

# Usage
import_configs = [
    {
        "name": "user-data",
        "sources": [
            {"type": "prefix", "uri": "s3://source/users/", "destination": "data/users/"}
        ],
        "validation": validate_csv_import
    },
    {
        "name": "config-files",
        "sources": [
            {"type": "object", "uri": "s3://source/app.yaml", "destination": "config/app.yaml"},
            {"type": "object", "uri": "s3://source/db.yaml", "destination": "config/db.yaml"}
        ]
    }
]

results = validated_batch_import(branch, import_configs)
```

### Incremental Import Pattern

```python
def incremental_import(branch, source_prefix, destination_prefix, last_import_marker=None):
    """Import only new data since last import"""
    
    # In a real implementation, you would track what was imported previously
    # This is a simplified example
    
    importer = branch.import_data(
        commit_message=f"Incremental import from {source_prefix}",
        metadata={
            "import_type": "incremental",
            "last_marker": last_import_marker or "none",
            "source": source_prefix
        }
    )
    
    # Add source (in practice, you'd filter based on modification time or other criteria)
    importer.prefix(source_prefix, destination=destination_prefix)
    
    result = importer.run()
    
    # Store marker for next incremental import
    new_marker = result.commit.id
    print(f"Incremental import completed. Next marker: {new_marker}")
    
    return result, new_marker

# Usage
result, marker = incremental_import(
    branch, 
    "s3://source/daily-data/", 
    "data/daily/"
)
```

## Export Operations

### Basic Export Patterns

```python
def export_to_s3(branch, prefix, s3_bucket, s3_prefix):
    """Export lakeFS objects to S3 (example pattern)"""
    import boto3
    
    s3_client = boto3.client('s3')
    exported_count = 0
    
    for obj_info in branch.objects(prefix=prefix):
        # Read from lakeFS
        obj = branch.object(obj_info.path)
        
        with obj.reader(mode='rb') as reader:
            content = reader.read()
        
        # Write to S3
        s3_key = s3_prefix + obj_info.path[len(prefix):]
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=content,
            ContentType=obj_info.content_type or 'application/octet-stream'
        )
        
        exported_count += 1
        print(f"Exported: {obj_info.path} -> s3://{s3_bucket}/{s3_key}")
    
    print(f"Export completed: {exported_count} objects")
    return exported_count

# Usage
export_count = export_to_s3(
    branch, 
    "data/processed/", 
    "export-bucket", 
    "lakefs-export/"
)
```

### Streaming Export for Large Files

```python
def streaming_export(branch, object_path, local_path):
    """Export large object using streaming"""
    
    obj = branch.object(object_path)
    
    with obj.reader(mode='rb') as reader:
        with open(local_path, 'wb') as writer:
            chunk_size = 64 * 1024  # 64KB chunks
            total_bytes = 0
            
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                
                writer.write(chunk)
                total_bytes += len(chunk)
                
                # Progress indicator
                if total_bytes % (1024 * 1024) == 0:  # Every MB
                    print(f"Exported {total_bytes // (1024 * 1024)} MB...")
    
    print(f"Export completed: {total_bytes} bytes -> {local_path}")

# Usage
streaming_export(branch, "data/large-dataset.dat", "exported-dataset.dat")
```

## Best Practices and Performance

### Large Dataset Import Best Practices

```python
def optimized_large_import(branch, source_uri, destination):
    """Best practices for importing large datasets"""
    
    # Use descriptive commit message with metadata
    importer = branch.import_data(
        commit_message=f"Large dataset import: {destination}",
        metadata={
            "source": source_uri,
            "import_strategy": "optimized",
            "expected_size": "large",
            "monitoring": "enabled"
        }
    )
    
    # Add source
    importer.prefix(source_uri, destination=destination)
    
    # Start import
    import_id = importer.start()
    print(f"Large import started: {import_id}")
    
    # Monitor with longer intervals for large imports
    poll_interval = 30  # 30 seconds
    last_count = 0
    
    while True:
        status = importer.status()
        
        if status.completed:
            print(f"✅ Large import completed: {status.ingested_objects} objects")
            return status
        
        if status.error:
            print(f"❌ Large import failed: {status.error.message}")
            return None
        
        # Show progress with rate calculation
        current_count = status.ingested_objects or 0
        rate = (current_count - last_count) / poll_interval
        print(f"⏳ Progress: {current_count} objects ({rate:.1f} objects/sec)")
        last_count = current_count
        
        time.sleep(poll_interval)

# Usage
result = optimized_large_import(
    branch, 
    "s3://massive-dataset/", 
    "data/massive/"
)
```

### Import Performance Monitoring

```python
class ImportPerformanceMonitor:
    """Monitor and log import performance metrics"""
    
    def __init__(self):
        self.start_time = None
        self.metrics = []
    
    def start_monitoring(self, importer):
        """Start monitoring an import operation"""
        self.start_time = time.time()
        import_id = importer.start()
        
        print(f"Monitoring import: {import_id}")
        return self._monitor_loop(importer)
    
    def _monitor_loop(self, importer):
        """Monitor import progress and collect metrics"""
        
        while True:
            current_time = time.time()
            status = importer.status()
            
            # Collect metrics
            metric = {
                "timestamp": current_time,
                "elapsed": current_time - self.start_time,
                "objects": status.ingested_objects or 0,
                "completed": status.completed,
                "error": status.error.message if status.error else None
            }
            self.metrics.append(metric)
            
            if status.completed:
                self._print_summary(status)
                return status
            
            if status.error:
                print(f"Import failed: {status.error.message}")
                return None
            
            # Progress update
            rate = metric["objects"] / metric["elapsed"] if metric["elapsed"] > 0 else 0
            print(f"Progress: {metric['objects']} objects, {rate:.1f} obj/sec")
            
            time.sleep(10)
    
    def _print_summary(self, final_status):
        """Print performance summary"""
        total_time = time.time() - self.start_time
        total_objects = final_status.ingested_objects
        avg_rate = total_objects / total_time if total_time > 0 else 0
        
        print(f"\n📊 Import Performance Summary:")
        print(f"   Total time: {total_time:.1f} seconds")
        print(f"   Total objects: {total_objects}")
        print(f"   Average rate: {avg_rate:.1f} objects/second")
        print(f"   Commit ID: {final_status.commit.id}")

# Usage
monitor = ImportPerformanceMonitor()

importer = branch.import_data(commit_message="Monitored import")
importer.prefix("s3://source-data/", destination="data/")

result = monitor.start_monitoring(importer)
```

## Key Points

- **Atomic operations**: All imports are committed atomically - either all data is imported or none
- **Progress monitoring**: Use asynchronous imports for large datasets with progress tracking
- **Error handling**: Implement comprehensive error handling and recovery strategies
- **Metadata**: Use rich metadata to track import sources, dates, and validation status
- **Performance**: Monitor import rates and optimize polling intervals for large datasets
- **Cancellation**: Long-running imports can be cancelled if needed
- **Validation**: Implement post-import validation to ensure data integrity

## See Also

- **[Repository Management](repositories.md)** - Creating and managing repositories
- **[Branch Operations](branches-and-commits.md)** - Version control operations
- **[Object Operations](objects-and-io.md)** - Individual object management
- **[Transactions](transactions.md)** - Atomic multi-operation workflows
- **[Best Practices](../reference/best-practices.md)** - Production deployment guidance