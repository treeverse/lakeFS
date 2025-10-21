---
title: Python - Transactions
description: Perform atomic operations using transactions in lakeFS with Python
---

# Working with Transactions

Transactions enable you to perform multiple operations atomically on lakeFS, similar to database transactions. This guide covers creating and managing transactions for reliable data operations.

## Prerequisites

- lakeFS server running and accessible
- Python SDK installed: `pip install lakefs`
- A repository with at least one branch (or create one in the examples)
- Proper credentials configured

## Understanding Transactions

### What are Transactions?

A transaction in lakeFS:

1. Creates an ephemeral (temporary) branch from a source branch
2. Performs all operations on that ephemeral branch
3. Atomically merges the branch back upon successful completion
4. Automatically cleans up the ephemeral branch
5. Rolls back if any error occurs

This ensures either all operations succeed or none of them do.

## Creating Transactions

### Basic Transaction

Perform multiple operations atomically:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

try:
    with branch.transact(commit_message="Add datasets") as tx:
        # All operations happen on ephemeral branch
        tx.object("data/file1.csv").upload(data=b"id,value\n1,100\n2,200")
        tx.object("data/file2.csv").upload(data=b"id,name\n1,Alice\n2,Bob")
        
        print("Upload successful - changes will be committed atomically")
    
    # At this point, transaction is complete and merged
    print("Transaction committed to main")
    
except Exception as e:
    # If we get here, changes were rolled back
    print(f"Transaction failed and rolled back: {e}")
```

### Transaction with Metadata

Include metadata in the commit created by the transaction:

```python
import lakefs
from datetime import datetime

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

try:
    with branch.transact(
        commit_message="Import customer data",
        commit_metadata={
            "import-date": datetime.now().isoformat(),
            "source": "database-export",
            "record-count": "10000"
        }
    ) as tx:
        # Perform operations
        tx.object("data/customers.csv").upload(data=b"id,name,email\n1,Alice,alice@example.com")
        
        print("Data imported")
    
    # Transaction complete with metadata
    print("Transaction committed with tracking metadata")
    
except Exception as e:
    print(f"Import failed: {e}")
```

### Transaction with Tagging

Create a tag after successful transaction completion:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

try:
    with branch.transact(
        commit_message="Production data release v1.5",
        tag="v1.5.0"  # Tag is created if transaction succeeds
    ) as tx:
        # Make changes
        tx.object("VERSION").upload(data=b"1.5.0")
        tx.object("data/prod.csv").upload(data=b"updated data")
        
        print("Production release in progress")
    
    print("Release tagged and deployed")
    
except Exception as e:
    print(f"Release failed: {e}")
    print("Changes rolled back, no tag created")
```

## Working with Transaction Context

### Performing Multiple Operations

Execute multiple operations within a transaction:

```python
import lakefs

repo = lakefs.repository("analytics-repo")
branch = repo.branch("develop")

try:
    with branch.transact(commit_message="Data preparation pipeline") as tx:
        # Step 1: Upload raw data
        tx.object("raw/input.csv").upload(data=b"raw input data")
        
        # Step 2: Upload processing script
        tx.object("scripts/transform.py").upload(
            data=b"#!/usr/bin/env python\n# Transformation logic"
        )
        
        # Step 3: Upload intermediate results
        tx.object("processed/output.csv").upload(data=b"processed output")
        
        # Step 4: Upload metadata
        tx.object(".metadata/pipeline_version.txt").upload(data=b"1.0")
        
        print("All pipeline stages added atomically")
    
    print("Pipeline committed successfully")
    
except Exception as e:
    print(f"Pipeline failed: {e}")
```

### Reading and Modifying Data

Read objects and modify them within a transaction:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

try:
    with branch.transact(commit_message="Data validation update") as tx:
        # Read existing data
        try:
            with tx.object("data/config.txt").reader(mode='r') as f:
                config_data = f.read()
            print(f"Current config: {config_data}")
        except:
            config_data = ""
        
        # Modify data
        updated_config = config_data + "\nvalidation_enabled: true"
        
        # Write updated data
        tx.object("data/config.txt").upload(data=updated_config.encode())
        
        print("Config updated")
    
    print("Configuration changes committed")
    
except Exception as e:
    print(f"Update failed: {e}")
```

### Iterating and Modifying Objects

Process multiple objects within a transaction:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

try:
    with branch.transact(commit_message="Bulk update data versions") as tx:
        # List objects and modify each one
        processed_count = 0
        
        for obj in tx.objects(prefix="data/"):
            # Read object
            try:
                with obj.reader(mode='r') as f:
                    content = f.read()
                
                # Add version marker
                versioned_content = f"# version: 2.0\n{content}"
                
                # Write back
                tx.object(obj.path).upload(data=versioned_content.encode())
                processed_count += 1
                
            except Exception as e:
                print(f"Error processing {obj.path}: {e}")
                raise  # Transaction will rollback
        
        print(f"Processed {processed_count} objects")
    
    print(f"Bulk update committed: {processed_count} objects updated")
    
except Exception as e:
    print(f"Bulk update failed and rolled back: {e}")
```

## Transaction Error Handling

### Basic Error Handling in Transactions

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

try:
    with branch.transact(commit_message="Data operations") as tx:
        # First operation
        tx.object("data/file1.csv").upload(data=b"data1")
        
        # Second operation that might fail
        try:
            with tx.object("data/large_file.csv").reader() as f:
                large_content = f.read()
            
            # Process large content
            if len(large_content) > 1000000:
                raise ValueError("File too large")
            
        except ValueError as e:
            print(f"Validation failed: {e}")
            raise  # This will rollback the entire transaction
        
        # Third operation
        tx.object("data/file2.csv").upload(data=b"data2")
        
except Exception as e:
    print(f"Transaction rolled back: {e}")
```

### Transaction with Cleanup on Error

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

cleanup_needed = False

try:
    with branch.transact(
        commit_message="Complex operation",
        delete_branch_on_error=True  # Default is True
    ) as tx:
        # Perform operations
        cleanup_needed = True
        
        tx.object("step1/file.csv").upload(data=b"step 1")
        
        # Simulate error
        if True:  # In real code, some condition
            raise Exception("Something went wrong in step 1")
        
        tx.object("step2/file.csv").upload(data=b"step 2")
        cleanup_needed = False
        
except Exception as e:
    if cleanup_needed:
        print(f"Operation failed, ephemeral branch automatically cleaned up")
    print(f"Error: {e}")
```

### Transaction with Conditional Rollback

```python
import lakefs

def validate_data(data):
    """Validate data meets requirements"""
    if len(data) == 0:
        raise ValueError("Empty data not allowed")
    if b"invalid" in data:
        raise ValueError("Data contains invalid markers")
    return True


repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

try:
    with branch.transact(commit_message="Data import with validation") as tx:
        # Upload data
        data = b"id,value\n1,100\n2,200"
        
        # Validate before committing
        validate_data(data)
        
        # If validation passes, commit
        tx.object("data/validated.csv").upload(data=data)
        
        print("Data validation passed, changes committed")
    
except ValueError as e:
    print(f"Validation error - transaction rolled back: {e}")
except Exception as e:
    print(f"Transaction error: {e}")
```

## Real-World Workflows

### Data Quality Validation Workflow

Implement data quality checks before committing changes:

```python
import lakefs
import csv
import io

def check_data_quality(repo_name, data_path, quality_rules):
    """
    Check data quality before committing
    Returns True if passes all checks, False otherwise
    """
    repo = lakefs.repository(repo_name)
    branch = repo.branch("main")
    
    try:
        with branch.transact(commit_message="Quality checked data") as tx:
            # Read data
            with tx.object(data_path).reader(mode='r') as f:
                data = f.read()
            
            # Run quality checks
            errors = []
            
            # Check 1: Not empty
            if len(data) == 0:
                errors.append("Data is empty")
            
            # Check 2: Valid CSV format
            try:
                reader = csv.reader(io.StringIO(data.decode()))
                rows = list(reader)
                if len(rows) < 2:
                    errors.append("Data has no rows")
            except Exception as e:
                errors.append(f"Invalid CSV format: {e}")
            
            # Check 3: Custom rules
            for rule in quality_rules:
                if not rule(data):
                    errors.append(f"Failed custom rule: {rule.__name__}")
            
            # If checks fail, raise error (transaction will rollback)
            if errors:
                error_msg = "; ".join(errors)
                raise ValueError(f"Quality checks failed: {error_msg}")
            
            # If all pass, add quality marker
            tx.object(f"{data_path}.quality_passed").upload(data=b"true")
            
            print(f"Data quality checks passed for {data_path}")
            return True
    
    except Exception as e:
        print(f"Quality check failed: {e}")
        return False


# Usage:
def rule_has_headers(data):
    """Custom rule: data must have headers"""
    lines = data.decode().strip().split('\n')
    return len(lines) > 0 and ',' in lines[0]

success = check_data_quality(
    "analytics-repo",
    "data/incoming.csv",
    [rule_has_headers]
)

if success:
    print("Data passed quality gates and is now committed")
```

### Database Synchronization Workflow

Keep data synchronized with atomicity guarantees:

```python
import lakefs
import json
from datetime import datetime

def sync_database_export(repo_name, table_name, export_data, export_metadata):
    """
    Atomically sync a database table export:
    1. Store the export data
    2. Update metadata
    3. Update sync timestamp
    4. All-or-nothing
    """
    repo = lakefs.repository(repo_name)
    branch = repo.branch("main")
    
    try:
        with branch.transact(
            commit_message=f"Sync: {table_name}",
            commit_metadata={
                "sync-type": "database-export",
                "table": table_name,
                "sync-time": datetime.now().isoformat()
            }
        ) as tx:
            # Step 1: Store the data
            data_path = f"data/{table_name}.csv"
            tx.object(data_path).upload(data=export_data)
            
            # Step 2: Store metadata
            metadata = {
                "table": table_name,
                "row_count": export_metadata.get("row_count", 0),
                "columns": export_metadata.get("columns", []),
                "sync_timestamp": datetime.now().isoformat(),
                "source_database": export_metadata.get("source", "unknown")
            }
            metadata_path = f".metadata/{table_name}_metadata.json"
            tx.object(metadata_path).upload(
                data=json.dumps(metadata, indent=2).encode()
            )
            
            # Step 3: Update sync status
            status = {
                "table": table_name,
                "last_sync": datetime.now().isoformat(),
                "status": "success"
            }
            status_path = f".sync/{table_name}_status.json"
            tx.object(status_path).upload(
                data=json.dumps(status).encode()
            )
            
            print(f"Synchronized {table_name}")
        
        print(f"Sync committed atomically")
        return True
        
    except Exception as e:
        print(f"Sync failed - rolling back: {e}")
        return False


# Usage:
success = sync_database_export(
    "warehouse-repo",
    "customers",
    b"id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com",
    {
        "row_count": 2,
        "columns": ["id", "name", "email"],
        "source": "production_db"
    }
)
```

### ETL Pipeline with Checkpoints

Implement ETL with atomic checkpoints:

```python
import lakefs

def etl_pipeline_step(repo_name, branch_name, step_name, step_logic):
    """
    Run an ETL step with atomic checkpointing
    """
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)
    
    try:
        with branch.transact(
            commit_message=f"ETL: {step_name}",
            commit_metadata={"etl-step": step_name}
        ) as tx:
            # Run the step logic
            result = step_logic(tx)
            
            # Create checkpoint
            checkpoint = {
                "step": step_name,
                "status": "completed",
                "records_processed": result.get("count", 0)
            }
            
            tx.object(f".checkpoints/{step_name}.json").upload(
                data=json.dumps(checkpoint).encode()
            )
            
            print(f"Step '{step_name}' completed with checkpoint")
            return True
            
    except Exception as e:
        print(f"Step '{step_name}' failed - checkpoint rolled back: {e}")
        return False


# Define ETL steps
def extract_step(tx):
    """Extract data"""
    tx.object("etl/01_raw/data.csv").upload(data=b"extracted data")
    return {"count": 1}

def transform_step(tx):
    """Transform data"""
    tx.object("etl/02_transformed/data.csv").upload(data=b"transformed data")
    return {"count": 1}

def load_step(tx):
    """Load data"""
    tx.object("etl/03_loaded/data.csv").upload(data=b"loaded data")
    return {"count": 1}


# Execute pipeline
import json

steps = [
    ("extract", extract_step),
    ("transform", transform_step),
    ("load", load_step)
]

for step_name, step_func in steps:
    success = etl_pipeline_step("data-repo", "main", step_name, step_func)
    if not success:
        print(f"ETL failed at {step_name}")
        break
else:
    print("ETL pipeline completed successfully")
```

### Schema Evolution with Validation

Safely evolve data schema with validation:

```python
import lakefs
import csv
import io

def evolve_schema(repo_name, table_name, old_schema, new_schema, migration_logic):
    """
    Evolve a table schema with validation
    Either all rows get migrated or none do
    """
    repo = lakefs.repository(repo_name)
    branch = repo.branch("main")
    
    try:
        with branch.transact(
            commit_message=f"Schema evolution: {table_name}",
            commit_metadata={
                "schema-version": new_schema.get("version"),
                "migration-type": "schema-evolution"
            }
        ) as tx:
            # Read current data
            try:
                with tx.object(f"data/{table_name}.csv").reader(mode='r') as f:
                    current_data = f.read()
            except:
                raise ValueError(f"Table {table_name} not found")
            
            # Parse CSV
            reader = csv.DictReader(io.StringIO(current_data.decode()))
            rows = list(reader)
            
            # Migrate each row
            migrated_rows = []
            for row in rows:
                migrated_row = migration_logic(row, old_schema, new_schema)
                migrated_rows.append(migrated_row)
            
            # Write migrated data
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=new_schema.get("columns", [])
            )
            writer.writeheader()
            writer.writerows(migrated_rows)
            
            tx.object(f"data/{table_name}.csv").upload(
                data=output.getvalue().encode()
            )
            
            # Store schema version
            tx.object(f".schema/{table_name}.json").upload(
                data=json.dumps(new_schema).encode()
            )
            
            print(f"Schema evolved for {table_name}")
            return True
            
    except Exception as e:
        print(f"Schema evolution failed - rolled back: {e}")
        return False


# Usage:
old_schema = {
    "version": 1,
    "columns": ["id", "name", "email"]
}

new_schema = {
    "version": 2,
    "columns": ["id", "name", "email", "phone"]
}

def add_phone_column(row, old, new):
    row["phone"] = ""  # Default empty phone
    return row

success = evolve_schema(
    "crm-repo",
    "contacts",
    old_schema,
    new_schema,
    add_phone_column
)
```

## Advanced Patterns

### Conditional Transaction Execution

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Check if we need to make changes
changes_needed = True  # In real code, some condition

if changes_needed:
    try:
        with branch.transact(commit_message="Conditional update") as tx:
            tx.object("data/file.csv").upload(data=b"new data")
            print("Update committed")
    except Exception as e:
        print(f"Update failed: {e}")
else:
    print("No changes needed, skipping transaction")
```

### Transaction Retry Logic

```python
import lakefs
import time

def transact_with_retry(repo_name, branch_name, max_retries=3):
    """Retry transaction on failure"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)
    
    for attempt in range(max_retries):
        try:
            with branch.transact(commit_message=f"Attempt {attempt + 1}") as tx:
                tx.object("data/file.csv").upload(data=b"data")
                print("Transaction succeeded")
                return True
                
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Failed after {max_retries} attempts")
                return False
```

## Error Handling

```python
import lakefs
from lakefs.exceptions import NotFoundException, ForbiddenException, ServerException

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

try:
    with branch.transact(commit_message="Safe operation") as tx:
        tx.object("data/file.csv").upload(data=b"data")
        
except NotFoundException:
    print("Branch or repository not found")
except ForbiddenException:
    print("Permission denied - cannot write to branch")
except ServerException as e:
    print(f"Server error - transaction rolled back: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```
