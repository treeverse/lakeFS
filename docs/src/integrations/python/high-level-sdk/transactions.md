---
title: Transaction Handling
description: Atomic operations using transactions in the High-Level Python SDK
sdk_types: ["high-level"]
difficulty: "intermediate"
use_cases: ["transactions", "atomic-operations", "data-consistency", "rollback"]
topics: ["transactions", "atomicity", "consistency", "rollback"]
audience: ["data-engineers", "developers", "python-developers"]
last_updated: "2024-01-15"
---

# Transaction Handling

Learn how to use transactions for atomic operations that ensure data consistency in your lakeFS repositories. Transactions provide ACID-like properties for complex multi-object operations.

## Transaction Concepts

### How Transactions Work
lakeFS transactions use an ephemeral branch pattern:
1. **Create ephemeral branch** - A temporary branch is created from the source branch
2. **Perform operations** - All changes are made on the ephemeral branch
3. **Commit changes** - Changes are committed to the ephemeral branch
4. **Merge back** - The ephemeral branch is merged back to the source branch
5. **Cleanup** - The ephemeral branch is deleted

### Transaction Properties
- **Atomic**: All operations succeed or all fail together
- **Consistent**: Repository remains in a valid state
- **Isolated**: Operations don't interfere with concurrent changes
- **Durable**: Committed changes are permanently stored

### Important Notes
- Transactions don't "lock" the source branch
- Concurrent changes to the source branch may cause transaction conflicts
- Transactions are designed for related operations that should succeed or fail together

## Basic Transaction Usage

### Simple Transaction Pattern

```python
import lakefs

branch = lakefs.repository("my-repo").branch("main")

# Basic transaction with context manager
with branch.transact(commit_message="Atomic data update") as tx:
    # All operations within this block are atomic
    tx.object("data/file1.txt").upload(data="New content 1")
    tx.object("data/file2.txt").upload(data="New content 2")
    
    # If we reach the end without exceptions, changes are committed
    # If an exception occurs, all changes are rolled back

print("Transaction completed successfully")
```

**Expected Output:**
```
Transaction completed successfully
```

### Transaction with Metadata

```python
# Transaction with rich metadata
with branch.transact(
    commit_message="Update user profiles and preferences",
    commit_metadata={
        "operation": "user_data_update",
        "version": "1.2.0",
        "updated_by": "data_pipeline",
        "batch_id": "batch_001"
    }
) as tx:
    # Upload user data
    tx.object("users/profiles.json").upload(
        data='{"users": [{"id": 1, "name": "Alice"}]}',
        content_type="application/json"
    )
    
    # Upload preferences
    tx.object("users/preferences.json").upload(
        data='{"theme": "dark", "notifications": true}',
        content_type="application/json"
    )

print("User data updated atomically")
```

### Accessing Transaction Properties

```python
with branch.transact(commit_message="Initial message") as tx:
    # Access transaction properties
    print(f"Transaction ID: {tx.id}")
    print(f"Source branch: {tx.source_id}")
    print(f"Commit message: {tx.commit_message}")
    
    # Modify transaction properties during execution
    tx.commit_message = "Updated commit message"
    tx.commit_metadata = {"updated": "true"}
    
    # Perform operations
    tx.object("data/example.txt").upload(data="Transaction example")
```

## Transaction Operations

### File Operations in Transactions

```python
with branch.transact(commit_message="Comprehensive file operations") as tx:
    # Upload new files
    tx.object("data/new-dataset.csv").upload(
        data="id,name,value\n1,Alice,100\n2,Bob,200",
        content_type="text/csv"
    )
    
    # Update existing files
    config_obj = tx.object("config/settings.json")
    if config_obj.exists():
        # Read current config
        current_config = json.loads(config_obj.reader().read())
        
        # Update configuration
        current_config["version"] = "2.0"
        current_config["last_updated"] = "2024-01-15"
        
        # Write updated config
        config_obj.upload(
            data=json.dumps(current_config, indent=2),
            content_type="application/json"
        )
    
    # Delete old files
    old_files = list(tx.objects(prefix="data/old/"))
    for obj_info in old_files:
        tx.object(obj_info.path).delete()
    
    # Batch delete using branch method
    temp_files = ["temp/file1.txt", "temp/file2.txt", "temp/file3.txt"]
    tx.delete_objects(temp_files)

print("File operations completed atomically")
```

### Data Processing Pipeline

```python
import json
import csv
from datetime import datetime

def process_data_pipeline(branch, input_path, output_path):
    """Process data through multiple stages atomically"""
    
    with branch.transact(
        commit_message=f"Data pipeline: {input_path} -> {output_path}",
        commit_metadata={
            "pipeline": "data_transformation",
            "input": input_path,
            "output": output_path,
            "timestamp": datetime.now().isoformat()
        }
    ) as tx:
        # Stage 1: Read and validate input
        input_obj = tx.object(input_path)
        if not input_obj.exists():
            raise ValueError(f"Input file {input_path} not found")
        
        raw_data = json.loads(input_obj.reader().read())
        
        # Stage 2: Transform data
        processed_records = []
        for record in raw_data.get("records", []):
            processed_record = {
                "id": record["id"],
                "name": record["name"].upper(),
                "value": record["value"] * 1.1,  # Apply 10% increase
                "processed_at": datetime.now().isoformat(),
                "status": "processed"
            }
            processed_records.append(processed_record)
        
        # Stage 3: Write processed data
        output_obj = tx.object(output_path)
        with output_obj.writer(mode='w', content_type="text/csv") as writer:
            if processed_records:
                fieldnames = processed_records[0].keys()
                csv_writer = csv.DictWriter(writer, fieldnames=fieldnames)
                csv_writer.writeheader()
                csv_writer.writerows(processed_records)
        
        # Stage 4: Create processing summary
        summary = {
            "input_file": input_path,
            "output_file": output_path,
            "records_processed": len(processed_records),
            "processing_time": datetime.now().isoformat(),
            "status": "completed"
        }
        
        summary_path = f"summaries/processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        tx.object(summary_path).upload(
            data=json.dumps(summary, indent=2),
            content_type="application/json"
        )
        
        # Stage 5: Archive input file
        archive_path = f"archive/{input_path}"
        input_obj.copy(tx.source_id, archive_path)
        input_obj.delete()
        
        return len(processed_records)

# Usage
processed_count = process_data_pipeline(
    branch, 
    "raw/user_data.json", 
    "processed/user_data.csv"
)
print(f"Pipeline completed: {processed_count} records processed")
```

## Error Handling and Rollback

### Automatic Rollback on Exceptions

```python
def demonstrate_rollback(branch):
    """Demonstrate automatic rollback on transaction failure"""
    
    # Check initial state
    initial_objects = list(branch.objects(prefix="demo/"))
    print(f"Initial objects: {len(initial_objects)}")
    
    try:
        with branch.transact(commit_message="Failing transaction") as tx:
            # These operations will be rolled back
            tx.object("demo/file1.txt").upload(data="Content 1")
            tx.object("demo/file2.txt").upload(data="Content 2")
            tx.object("demo/file3.txt").upload(data="Content 3")
            
            print("Files uploaded in transaction...")
            
            # Check objects within transaction
            tx_objects = list(tx.objects(prefix="demo/"))
            print(f"Objects in transaction: {len(tx_objects)}")
            
            # This will cause the transaction to fail and rollback
            raise ValueError("Simulated error - transaction will rollback!")
            
            # This line won't be reached
            tx.object("demo/file4.txt").upload(data="Content 4")
            
    except ValueError as e:
        print(f"Transaction failed: {e}")
    
    # Check final state - should be same as initial
    final_objects = list(branch.objects(prefix="demo/"))
    print(f"Final objects: {len(final_objects)}")
    print(f"Rollback successful: {len(initial_objects) == len(final_objects)}")

# Run demonstration
demonstrate_rollback(branch)
```

**Expected Output:**
```
Initial objects: 0
Files uploaded in transaction...
Objects in transaction: 3
Transaction failed: Simulated error - transaction will rollback!
Final objects: 0
Rollback successful: True
```

### Conditional Transaction Execution

```python
def conditional_update(branch, condition_check):
    """Execute transaction only if conditions are met"""
    
    with branch.transact(commit_message="Conditional update") as tx:
        # Check conditions before proceeding
        config_obj = tx.object("config/settings.json")
        
        if not config_obj.exists():
            raise ValueError("Configuration file not found")
        
        config = json.loads(config_obj.reader().read())
        
        # Apply condition check
        if not condition_check(config):
            raise ValueError("Conditions not met for update")
        
        # Proceed with updates
        config["last_updated"] = datetime.now().isoformat()
        config["update_count"] = config.get("update_count", 0) + 1
        
        config_obj.upload(
            data=json.dumps(config, indent=2),
            content_type="application/json"
        )
        
        # Log the update
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "action": "conditional_update",
            "config_version": config.get("version", "unknown")
        }
        
        tx.object("logs/updates.json").upload(
            data=json.dumps(log_entry),
            content_type="application/json"
        )
        
        return config

# Example condition check
def version_check(config):
    return config.get("version", 0) < 3

try:
    updated_config = conditional_update(branch, version_check)
    print("Conditional update successful")
except ValueError as e:
    print(f"Update skipped: {e}")
```

### Transaction Retry Pattern

```python
from lakefs.exceptions import TransactionException
import time
import random

def retry_transaction(branch, operation_func, max_retries=3, base_delay=1):
    """Execute transaction with exponential backoff retry"""
    
    for attempt in range(max_retries):
        try:
            with branch.transact(
                commit_message=f"Retry operation (attempt {attempt + 1})"
            ) as tx:
                result = operation_func(tx)
                print(f"Transaction succeeded on attempt {attempt + 1}")
                return result
                
        except TransactionException as e:
            if attempt == max_retries - 1:
                print(f"Transaction failed after {max_retries} attempts")
                raise
            
            # Exponential backoff with jitter
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)
        
        except Exception as e:
            # Non-transaction errors shouldn't be retried
            print(f"Non-retryable error: {e}")
            raise

def risky_operation(tx):
    """Simulate an operation that might fail due to conflicts"""
    # Simulate random failure
    if random.random() < 0.7:  # 70% chance of failure
        raise TransactionException("Simulated conflict")
    
    tx.object("data/result.txt").upload(data="Operation successful")
    return "success"

# Usage
try:
    result = retry_transaction(branch, risky_operation)
    print(f"Final result: {result}")
except Exception as e:
    print(f"Operation ultimately failed: {e}")
```

## Advanced Transaction Patterns

### Multi-Stage Data Processing

```python
class DataProcessor:
    """Multi-stage data processor using transactions"""
    
    def __init__(self, branch):
        self.branch = branch
    
    def process_dataset(self, dataset_name, stages):
        """Process dataset through multiple stages atomically"""
        
        with self.branch.transact(
            commit_message=f"Process dataset: {dataset_name}",
            commit_metadata={
                "dataset": dataset_name,
                "stages": len(stages),
                "processor": "DataProcessor"
            }
        ) as tx:
            
            current_data = None
            stage_results = []
            
            for i, stage in enumerate(stages):
                stage_name = stage.__name__
                print(f"Executing stage {i+1}: {stage_name}")
                
                try:
                    # Execute stage
                    stage_result = stage(tx, current_data, dataset_name)
                    current_data = stage_result.get("data")
                    stage_results.append({
                        "stage": stage_name,
                        "status": "success",
                        "output": stage_result.get("output_path"),
                        "records": stage_result.get("record_count", 0)
                    })
                    
                except Exception as e:
                    print(f"Stage {stage_name} failed: {e}")
                    raise  # This will rollback the entire transaction
            
            # Create processing report
            report = {
                "dataset": dataset_name,
                "processing_time": datetime.now().isoformat(),
                "stages_completed": len(stage_results),
                "stage_results": stage_results,
                "status": "completed"
            }
            
            report_path = f"reports/{dataset_name}_processing_report.json"
            tx.object(report_path).upload(
                data=json.dumps(report, indent=2),
                content_type="application/json"
            )
            
            return report

def extract_stage(tx, previous_data, dataset_name):
    """Extract data from source"""
    source_path = f"raw/{dataset_name}.json"
    source_obj = tx.object(source_path)
    
    if not source_obj.exists():
        raise ValueError(f"Source data not found: {source_path}")
    
    data = json.loads(source_obj.reader().read())
    
    return {
        "data": data,
        "output_path": source_path,
        "record_count": len(data.get("records", []))
    }

def transform_stage(tx, input_data, dataset_name):
    """Transform extracted data"""
    if not input_data:
        raise ValueError("No input data for transformation")
    
    # Transform records
    transformed_records = []
    for record in input_data.get("records", []):
        transformed_record = {
            "id": record["id"],
            "name": record["name"].strip().title(),
            "value": float(record["value"]) * 1.1,
            "transformed_at": datetime.now().isoformat()
        }
        transformed_records.append(transformed_record)
    
    # Save transformed data
    output_path = f"transformed/{dataset_name}.json"
    transformed_data = {"records": transformed_records}
    
    tx.object(output_path).upload(
        data=json.dumps(transformed_data, indent=2),
        content_type="application/json"
    )
    
    return {
        "data": transformed_data,
        "output_path": output_path,
        "record_count": len(transformed_records)
    }

def load_stage(tx, input_data, dataset_name):
    """Load transformed data to final destination"""
    if not input_data:
        raise ValueError("No input data for loading")
    
    # Convert to CSV format
    output_path = f"processed/{dataset_name}.csv"
    
    with tx.object(output_path).writer(mode='w', content_type="text/csv") as writer:
        records = input_data.get("records", [])
        if records:
            fieldnames = records[0].keys()
            csv_writer = csv.DictWriter(writer, fieldnames=fieldnames)
            csv_writer.writeheader()
            csv_writer.writerows(records)
    
    return {
        "data": input_data,
        "output_path": output_path,
        "record_count": len(input_data.get("records", []))
    }

# Usage
processor = DataProcessor(branch)
stages = [extract_stage, transform_stage, load_stage]

try:
    report = processor.process_dataset("user_data", stages)
    print(f"Processing completed: {report['stages_completed']} stages")
except Exception as e:
    print(f"Processing failed and rolled back: {e}")
```

### Concurrent Transaction Handling

```python
import threading
import queue
import time

class TransactionWorker:
    """Worker for handling concurrent transactions"""
    
    def __init__(self, branch, worker_id):
        self.branch = branch
        self.worker_id = worker_id
        self.results = []
    
    def process_work_item(self, work_item):
        """Process a single work item in a transaction"""
        
        item_id = work_item["id"]
        operation = work_item["operation"]
        data = work_item["data"]
        
        try:
            with self.branch.transact(
                commit_message=f"Worker {self.worker_id}: Process item {item_id}",
                commit_metadata={
                    "worker_id": str(self.worker_id),
                    "item_id": str(item_id),
                    "operation": operation
                }
            ) as tx:
                
                if operation == "create":
                    path = f"work_items/{item_id}.json"
                    tx.object(path).upload(
                        data=json.dumps(data, indent=2),
                        content_type="application/json"
                    )
                    
                elif operation == "update":
                    path = f"work_items/{item_id}.json"
                    existing_obj = tx.object(path)
                    
                    if existing_obj.exists():
                        existing_data = json.loads(existing_obj.reader().read())
                        existing_data.update(data)
                        existing_data["updated_by"] = f"worker_{self.worker_id}"
                        existing_data["updated_at"] = datetime.now().isoformat()
                        
                        existing_obj.upload(
                            data=json.dumps(existing_data, indent=2),
                            content_type="application/json"
                        )
                    else:
                        raise ValueError(f"Item {item_id} not found for update")
                
                elif operation == "delete":
                    path = f"work_items/{item_id}.json"
                    tx.object(path).delete()
                
                # Log the operation
                log_entry = {
                    "worker_id": self.worker_id,
                    "item_id": item_id,
                    "operation": operation,
                    "timestamp": datetime.now().isoformat(),
                    "status": "completed"
                }
                
                log_path = f"logs/worker_{self.worker_id}_{int(time.time())}.json"
                tx.object(log_path).upload(
                    data=json.dumps(log_entry),
                    content_type="application/json"
                )
                
                return {"status": "success", "item_id": item_id}
                
        except Exception as e:
            return {"status": "error", "item_id": item_id, "error": str(e)}

def concurrent_transaction_demo(branch, work_items, num_workers=3):
    """Demonstrate concurrent transaction processing"""
    
    work_queue = queue.Queue()
    result_queue = queue.Queue()
    
    # Add work items to queue
    for item in work_items:
        work_queue.put(item)
    
    def worker_thread(worker_id):
        """Worker thread function"""
        worker = TransactionWorker(branch, worker_id)
        
        while True:
            try:
                work_item = work_queue.get(timeout=1)
                result = worker.process_work_item(work_item)
                result_queue.put(result)
                work_queue.task_done()
                
            except queue.Empty:
                break
            except Exception as e:
                result_queue.put({
                    "status": "error", 
                    "worker_id": worker_id, 
                    "error": str(e)
                })
    
    # Start worker threads
    threads = []
    for i in range(num_workers):
        thread = threading.Thread(target=worker_thread, args=(i,))
        thread.start()
        threads.append(thread)
    
    # Wait for completion
    work_queue.join()
    
    # Collect results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # Wait for threads to finish
    for thread in threads:
        thread.join()
    
    return results

# Example usage
work_items = [
    {"id": 1, "operation": "create", "data": {"name": "Item 1", "value": 100}},
    {"id": 2, "operation": "create", "data": {"name": "Item 2", "value": 200}},
    {"id": 3, "operation": "create", "data": {"name": "Item 3", "value": 300}},
    {"id": 1, "operation": "update", "data": {"value": 150}},
    {"id": 2, "operation": "delete", "data": {}},
]

results = concurrent_transaction_demo(branch, work_items)
print(f"Processed {len(results)} items concurrently")

success_count = len([r for r in results if r["status"] == "success"])
error_count = len([r for r in results if r["status"] == "error"])
print(f"Success: {success_count}, Errors: {error_count}")
```

## Transaction Best Practices

### Transaction Scope and Size

```python
# ✅ Good: Focused, related operations
def good_transaction_example(branch):
    with branch.transact(commit_message="Update user profile") as tx:
        # Related operations that should succeed/fail together
        tx.object("users/profile.json").upload(data=profile_data)
        tx.object("users/preferences.json").upload(data=preferences_data)
        tx.object("users/index.json").upload(data=updated_index)

# ❌ Avoid: Unrelated operations in single transaction
def avoid_large_transaction(branch):
    with branch.transact(commit_message="Daily batch processing") as tx:
        # Too many unrelated operations
        # This makes the transaction prone to conflicts and hard to debug
        for i in range(1000):  # Too many operations
            tx.object(f"data/file_{i}.txt").upload(data=f"content {i}")
        
        # Unrelated operations mixed together
        tx.object("config/settings.json").upload(data=config_data)
        tx.object("logs/daily.log").upload(data=log_data)
```

### Optimal Batch Sizes

```python
def optimal_batch_processing(branch, items, batch_size=50):
    """Process items in optimal batch sizes"""
    
    total_batches = (len(items) + batch_size - 1) // batch_size
    results = []
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(items))
        batch_items = items[start_idx:end_idx]
        
        try:
            with branch.transact(
                commit_message=f"Process batch {batch_num + 1}/{total_batches}",
                commit_metadata={
                    "batch_number": str(batch_num + 1),
                    "total_batches": str(total_batches),
                    "items_in_batch": str(len(batch_items))
                }
            ) as tx:
                
                batch_results = []
                for item in batch_items:
                    # Process individual item
                    result = process_item(tx, item)
                    batch_results.append(result)
                
                # Create batch summary
                summary = {
                    "batch_number": batch_num + 1,
                    "items_processed": len(batch_results),
                    "timestamp": datetime.now().isoformat()
                }
                
                tx.object(f"summaries/batch_{batch_num + 1}.json").upload(
                    data=json.dumps(summary, indent=2),
                    content_type="application/json"
                )
                
                results.extend(batch_results)
                print(f"Completed batch {batch_num + 1}/{total_batches}")
                
        except Exception as e:
            print(f"Batch {batch_num + 1} failed: {e}")
            # Continue with next batch rather than failing everything
            continue
    
    return results

def process_item(tx, item):
    """Process a single item within transaction"""
    path = f"processed/{item['id']}.json"
    tx.object(path).upload(
        data=json.dumps(item, indent=2),
        content_type="application/json"
    )
    return {"id": item["id"], "status": "processed"}
```

### Error Handling Strategies

```python
class TransactionManager:
    """Advanced transaction management with error handling"""
    
    def __init__(self, branch):
        self.branch = branch
        self.failed_operations = []
    
    def execute_with_fallback(self, primary_operation, fallback_operation, 
                            commit_message):
        """Execute operation with fallback on failure"""
        
        # Try primary operation
        try:
            with self.branch.transact(commit_message=commit_message) as tx:
                result = primary_operation(tx)
                return {"status": "success", "method": "primary", "result": result}
                
        except Exception as primary_error:
            print(f"Primary operation failed: {primary_error}")
            
            # Try fallback operation
            try:
                with self.branch.transact(
                    commit_message=f"{commit_message} (fallback)"
                ) as tx:
                    result = fallback_operation(tx)
                    return {"status": "success", "method": "fallback", "result": result}
                    
            except Exception as fallback_error:
                print(f"Fallback operation also failed: {fallback_error}")
                self.failed_operations.append({
                    "commit_message": commit_message,
                    "primary_error": str(primary_error),
                    "fallback_error": str(fallback_error),
                    "timestamp": datetime.now().isoformat()
                })
                return {"status": "failed", "errors": [primary_error, fallback_error]}
    
    def get_failed_operations(self):
        """Get list of failed operations for analysis"""
        return self.failed_operations

# Example usage
def primary_data_update(tx):
    """Primary method - more complex but preferred"""
    # Complex data transformation
    tx.object("data/complex_result.json").upload(data='{"method": "complex"}')
    return "complex_processing_complete"

def fallback_data_update(tx):
    """Fallback method - simpler but reliable"""
    # Simple data update
    tx.object("data/simple_result.json").upload(data='{"method": "simple"}')
    return "simple_processing_complete"

manager = TransactionManager(branch)
result = manager.execute_with_fallback(
    primary_data_update,
    fallback_data_update,
    "Update data with fallback"
)

print(f"Operation result: {result}")
```

### Transaction Monitoring and Debugging

```python
class TransactionMonitor:
    """Monitor transaction performance and behavior"""
    
    def __init__(self):
        self.transaction_logs = []
    
    def monitored_transaction(self, branch, commit_message, operation_func, 
                            commit_metadata=None):
        """Execute transaction with monitoring"""
        
        start_time = time.time()
        transaction_id = f"tx_{int(start_time)}"
        
        log_entry = {
            "transaction_id": transaction_id,
            "commit_message": commit_message,
            "start_time": start_time,
            "metadata": commit_metadata
        }
        
        try:
            with branch.transact(
                commit_message=commit_message,
                commit_metadata=commit_metadata
            ) as tx:
                # Log transaction start
                print(f"🚀 Starting transaction: {transaction_id}")
                
                # Execute operation
                result = operation_func(tx)
                
                # Calculate duration
                duration = time.time() - start_time
                
                # Log success
                log_entry.update({
                    "status": "success",
                    "duration": duration,
                    "end_time": time.time(),
                    "result": str(result)[:100]  # Truncate long results
                })
                
                print(f"✅ Transaction completed: {transaction_id} ({duration:.2f}s)")
                
                return result
                
        except Exception as e:
            # Calculate duration
            duration = time.time() - start_time
            
            # Log failure
            log_entry.update({
                "status": "failed",
                "duration": duration,
                "end_time": time.time(),
                "error": str(e)
            })
            
            print(f"❌ Transaction failed: {transaction_id} ({duration:.2f}s) - {e}")
            raise
            
        finally:
            self.transaction_logs.append(log_entry)
    
    def get_transaction_stats(self):
        """Get transaction statistics"""
        if not self.transaction_logs:
            return {"message": "No transactions recorded"}
        
        successful = [log for log in self.transaction_logs if log["status"] == "success"]
        failed = [log for log in self.transaction_logs if log["status"] == "failed"]
        
        avg_duration = sum(log["duration"] for log in self.transaction_logs) / len(self.transaction_logs)
        
        return {
            "total_transactions": len(self.transaction_logs),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": len(successful) / len(self.transaction_logs) * 100,
            "average_duration": avg_duration,
            "longest_transaction": max(self.transaction_logs, key=lambda x: x["duration"])["duration"],
            "shortest_transaction": min(self.transaction_logs, key=lambda x: x["duration"])["duration"]
        }

# Usage example
monitor = TransactionMonitor()

def sample_operation(tx):
    tx.object("test/file1.txt").upload(data="test content 1")
    tx.object("test/file2.txt").upload(data="test content 2")
    time.sleep(0.1)  # Simulate processing time
    return "operation_complete"

# Execute monitored transactions
for i in range(5):
    try:
        monitor.monitored_transaction(
            branch,
            f"Test transaction {i+1}",
            sample_operation,
            {"test_run": str(i+1)}
        )
    except Exception:
        pass  # Continue with other transactions

# Get statistics
stats = monitor.get_transaction_stats()
print(f"\n📊 Transaction Statistics:")
for key, value in stats.items():
    print(f"   {key}: {value}")
```

## Key Points

- **Atomic operations**: All changes in a transaction succeed or fail together
- **Ephemeral branches**: Transactions use temporary branches for isolation
- **No locking**: Transactions don't lock the source branch, conflicts are possible
- **Context managers**: Always use `with` statements for proper cleanup
- **Error handling**: Implement comprehensive error handling and fallback strategies
- **Batch sizing**: Keep transactions focused and reasonably sized
- **Monitoring**: Monitor transaction performance and success rates
- **Rollback**: Failed transactions automatically clean up ephemeral branches

## See Also

- **[Repository Management](repositories.md)** - Creating and managing repositories
- **[Branch Operations](branches-and-commits.md)** - Version control operations
- **[Object Operations](objects-and-io.md)** - Individual object management
- **[Import Operations](imports-and-exports.md)** - Bulk data operations
- **[Advanced Features](advanced.md)** - Advanced patterns and optimization
- **[Best Practices](../reference/best-practices.md)** - Production deployment guidance