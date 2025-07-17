---
title: Transaction Patterns with lakefs-spec
description: Atomic operations and transaction handling using lakefs-spec
sdk_types: ["lakefs-spec"]
difficulty: "intermediate"
use_cases: ["transactions", "atomic-operations", "data-consistency", "rollback"]
topics: ["transactions", "atomicity", "consistency", "context-managers"]
audience: ["data-scientists", "data-engineers", "python-developers"]
last_updated: "2024-01-15"
---

# Transaction Patterns with lakefs-spec

lakefs-spec provides transaction support for atomic operations, ensuring data consistency across multiple file operations.

## Transaction Basics

### Simple Transaction
```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# Basic transaction pattern
with fs.transaction("my-repo", "main") as tx:
    # All operations within this block are atomic
    fs.write_text(f"my-repo/{tx.branch.id}/data/file1.txt", "Content 1")
    fs.write_text(f"my-repo/{tx.branch.id}/data/file2.txt", "Content 2")
    
    # Commit changes
    tx.commit(message="Add two files atomically")
```

### Transaction with Multiple Commits
```python
with fs.transaction("my-repo", "main") as tx:
    # First set of changes
    fs.write_text(f"my-repo/{tx.branch.id}/step1/data.txt", "Step 1 data")
    tx.commit(message="Complete step 1")
    
    # Second set of changes
    fs.write_text(f"my-repo/{tx.branch.id}/step2/data.txt", "Step 2 data")
    tx.commit(message="Complete step 2")
    
    # Create a tag for the final state
    final_commit = tx.commit(message="Final transaction state")
    tx.tag(final_commit, name="transaction-v1.0")
```

## Data Processing Transactions

### ETL Pipeline with Transactions
```python
import pandas as pd
from lakefs_spec import LakeFSFileSystem

def atomic_etl_pipeline(repo, source_branch, raw_data_path):
    """Perform ETL operations atomically"""
    fs = LakeFSFileSystem()
    
    with fs.transaction(repo, source_branch) as tx:
        branch_path = f"{repo}/{tx.branch.id}"
        
        # Extract: Read raw data
        raw_df = pd.read_csv(f"lakefs://{branch_path}/{raw_data_path}")
        
        # Transform: Clean and process data
        # Remove duplicates
        clean_df = raw_df.drop_duplicates()
        
        # Handle missing values
        clean_df = clean_df.fillna(method='forward')
        
        # Add derived columns
        clean_df['processed_date'] = pd.Timestamp.now()
        clean_df['record_count'] = len(clean_df)
        
        # Load: Save processed data
        clean_df.to_parquet(f"lakefs://{branch_path}/processed/clean_data.parquet", index=False)
        
        # Create summary statistics
        summary = clean_df.describe()
        summary.to_csv(f"lakefs://{branch_path}/processed/summary_stats.csv")
        
        # Commit all changes atomically
        commit_sha = tx.commit(message=f"ETL pipeline: processed {len(clean_df)} records")
        
        # Tag the successful processing
        tx.tag(commit_sha, name=f"etl-{pd.Timestamp.now().strftime('%Y%m%d-%H%M%S')}")
        
        return len(clean_df), commit_sha

# Usage
try:
    record_count, commit_id = atomic_etl_pipeline("my-repo", "main", "raw/sales_data.csv")
    print(f"Successfully processed {record_count} records in commit {commit_id}")
except Exception as e:
    print(f"ETL pipeline failed: {e}")
    # All changes are automatically rolled back
```

### Multi-Dataset Processing
```python
def process_multiple_datasets(repo, branch, dataset_configs):
    """Process multiple datasets atomically"""
    fs = LakeFSFileSystem()
    
    with fs.transaction(repo, branch) as tx:
        branch_path = f"{repo}/{tx.branch.id}"
        processed_datasets = []
        
        for config in dataset_configs:
            try:
                # Read source data
                if config['format'] == 'csv':
                    df = pd.read_csv(f"lakefs://{branch_path}/{config['source_path']}")
                elif config['format'] == 'parquet':
                    df = pd.read_parquet(f"lakefs://{branch_path}/{config['source_path']}")
                
                # Apply transformations
                for transform in config.get('transformations', []):
                    df = apply_transformation(df, transform)
                
                # Save processed data
                output_path = f"lakefs://{branch_path}/{config['output_path']}"
                if config['output_format'] == 'parquet':
                    df.to_parquet(output_path, index=False)
                elif config['output_format'] == 'csv':
                    df.to_csv(output_path, index=False)
                
                processed_datasets.append({
                    'name': config['name'],
                    'records': len(df),
                    'output_path': config['output_path']
                })
                
            except Exception as e:
                print(f"Failed to process dataset {config['name']}: {e}")
                raise  # This will rollback the entire transaction
        
        # Create processing summary
        summary_df = pd.DataFrame(processed_datasets)
        summary_df.to_csv(f"lakefs://{branch_path}/processing_summary.csv", index=False)
        
        # Commit all changes
        total_records = sum(d['records'] for d in processed_datasets)
        commit_sha = tx.commit(
            message=f"Processed {len(processed_datasets)} datasets ({total_records} total records)"
        )
        
        return processed_datasets, commit_sha

def apply_transformation(df, transform):
    """Apply a transformation to a DataFrame"""
    if transform['type'] == 'filter':
        return df.query(transform['condition'])
    elif transform['type'] == 'select':
        return df[transform['columns']]
    elif transform['type'] == 'rename':
        return df.rename(columns=transform['mapping'])
    else:
        return df

# Usage
dataset_configs = [
    {
        'name': 'sales',
        'source_path': 'raw/sales.csv',
        'format': 'csv',
        'output_path': 'processed/sales_clean.parquet',
        'output_format': 'parquet',
        'transformations': [
            {'type': 'filter', 'condition': 'amount > 0'},
            {'type': 'select', 'columns': ['date', 'amount', 'customer_id']}
        ]
    },
    {
        'name': 'customers',
        'source_path': 'raw/customers.parquet',
        'format': 'parquet',
        'output_path': 'processed/customers_clean.parquet',
        'output_format': 'parquet',
        'transformations': [
            {'type': 'rename', 'mapping': {'cust_id': 'customer_id'}}
        ]
    }
]

try:
    results, commit_id = process_multiple_datasets("my-repo", "main", dataset_configs)
    print(f"Successfully processed {len(results)} datasets in commit {commit_id}")
except Exception as e:
    print(f"Multi-dataset processing failed: {e}")
```

## Machine Learning Workflows

### Model Training with Transactions
```python
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import joblib
import json

def atomic_ml_training(repo, branch, data_path, model_config):
    """Train ML model atomically with data versioning"""
    fs = LakeFSFileSystem()
    
    with fs.transaction(repo, branch) as tx:
        branch_path = f"{repo}/{tx.branch.id}"
        
        # Load training data
        df = pd.read_parquet(f"lakefs://{branch_path}/{data_path}")
        
        # Prepare features and target
        X = df.drop(model_config['target_column'], axis=1)
        y = df[model_config['target_column']]
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=model_config.get('test_size', 0.2), random_state=42
        )
        
        # Train model
        model = RandomForestClassifier(**model_config.get('model_params', {}))
        model.fit(X_train, y_train)
        
        # Evaluate model
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        # Save model artifacts
        model_dir = f"lakefs://{branch_path}/models/{model_config['model_name']}"
        
        # Save the trained model
        with fs.open(f"{model_dir}/model.pkl", 'wb') as f:
            joblib.dump(model, f)
        
        # Save model metadata
        metadata = {
            'model_name': model_config['model_name'],
            'accuracy': accuracy,
            'training_records': len(X_train),
            'test_records': len(X_test),
            'features': list(X.columns),
            'model_params': model_config.get('model_params', {}),
            'training_date': pd.Timestamp.now().isoformat()
        }
        
        with fs.open(f"{model_dir}/metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Save test results
        results_df = pd.DataFrame({
            'actual': y_test,
            'predicted': y_pred
        })
        results_df.to_csv(f"{model_dir}/test_results.csv", index=False)
        
        # Save classification report
        report = classification_report(y_test, y_pred, output_dict=True)
        with fs.open(f"{model_dir}/classification_report.json", 'w') as f:
            json.dump(report, f, indent=2)
        
        # Commit model training results
        commit_sha = tx.commit(
            message=f"Train {model_config['model_name']} model (accuracy: {accuracy:.3f})"
        )
        
        # Tag the model version
        model_tag = f"{model_config['model_name']}-v{pd.Timestamp.now().strftime('%Y%m%d-%H%M%S')}"
        tx.tag(commit_sha, name=model_tag)
        
        return {
            'accuracy': accuracy,
            'commit_sha': commit_sha,
            'model_tag': model_tag,
            'metadata': metadata
        }

# Usage
model_config = {
    'model_name': 'customer_churn_predictor',
    'target_column': 'churn',
    'test_size': 0.2,
    'model_params': {
        'n_estimators': 100,
        'max_depth': 10,
        'random_state': 42
    }
}

try:
    results = atomic_ml_training("my-repo", "main", "processed/customer_data.parquet", model_config)
    print(f"Model training completed successfully:")
    print(f"  Accuracy: {results['accuracy']:.3f}")
    print(f"  Commit: {results['commit_sha']}")
    print(f"  Tag: {results['model_tag']}")
except Exception as e:
    print(f"Model training failed: {e}")
```

## Error Handling and Recovery

### Robust Transaction Patterns
```python
def robust_data_processing(repo, branch, operations):
    """Process data with comprehensive error handling"""
    fs = LakeFSFileSystem()
    
    try:
        with fs.transaction(repo, branch) as tx:
            branch_path = f"{repo}/{tx.branch.id}"
            completed_operations = []
            
            for i, operation in enumerate(operations):
                try:
                    # Execute operation
                    result = execute_operation(fs, branch_path, operation)
                    completed_operations.append({
                        'operation': operation['name'],
                        'status': 'success',
                        'result': result
                    })
                    
                    # Intermediate commit for long-running processes
                    if i % 5 == 4:  # Commit every 5 operations
                        tx.commit(message=f"Completed operations {i-4} to {i}")
                    
                except Exception as e:
                    print(f"Operation {operation['name']} failed: {e}")
                    completed_operations.append({
                        'operation': operation['name'],
                        'status': 'failed',
                        'error': str(e)
                    })
                    
                    # Decide whether to continue or abort
                    if operation.get('critical', False):
                        raise  # Abort transaction for critical operations
                    # Continue for non-critical operations
            
            # Save processing log
            log_df = pd.DataFrame(completed_operations)
            log_df.to_csv(f"lakefs://{branch_path}/processing_log.csv", index=False)
            
            # Final commit
            successful_ops = sum(1 for op in completed_operations if op['status'] == 'success')
            failed_ops = len(completed_operations) - successful_ops
            
            commit_sha = tx.commit(
                message=f"Batch processing: {successful_ops} successful, {failed_ops} failed"
            )
            
            return completed_operations, commit_sha
            
    except Exception as e:
        print(f"Transaction failed and rolled back: {e}")
        return None, None

def execute_operation(fs, branch_path, operation):
    """Execute a single operation"""
    if operation['type'] == 'transform':
        # Load, transform, and save data
        df = pd.read_csv(f"lakefs://{branch_path}/{operation['input_path']}")
        # Apply transformation logic here
        transformed_df = df  # Placeholder
        transformed_df.to_parquet(f"lakefs://{branch_path}/{operation['output_path']}", index=False)
        return len(transformed_df)
    
    elif operation['type'] == 'aggregate':
        # Aggregate data
        df = pd.read_parquet(f"lakefs://{branch_path}/{operation['input_path']}")
        aggregated = df.groupby(operation['group_by']).sum()
        aggregated.to_csv(f"lakefs://{branch_path}/{operation['output_path']}")
        return len(aggregated)
    
    else:
        raise ValueError(f"Unknown operation type: {operation['type']}")

# Usage
operations = [
    {
        'name': 'clean_sales_data',
        'type': 'transform',
        'input_path': 'raw/sales.csv',
        'output_path': 'processed/sales_clean.parquet',
        'critical': True
    },
    {
        'name': 'aggregate_by_region',
        'type': 'aggregate',
        'input_path': 'processed/sales_clean.parquet',
        'output_path': 'aggregated/sales_by_region.csv',
        'group_by': 'region',
        'critical': False
    }
]

results, commit_id = robust_data_processing("my-repo", "main", operations)
if results:
    print(f"Processing completed with commit {commit_id}")
    for result in results:
        print(f"  {result['operation']}: {result['status']}")
```

## Advanced Transaction Patterns

### Conditional Transactions
```python
def conditional_data_update(repo, branch, condition_check, update_operations):
    """Perform updates only if conditions are met"""
    fs = LakeFSFileSystem()
    
    with fs.transaction(repo, branch) as tx:
        branch_path = f"{repo}/{tx.branch.id}"
        
        # Check conditions
        condition_met = check_conditions(fs, branch_path, condition_check)
        
        if not condition_met:
            print("Conditions not met, skipping updates")
            return None
        
        # Perform updates
        for operation in update_operations:
            execute_update(fs, branch_path, operation)
        
        commit_sha = tx.commit(message="Conditional update completed")
        return commit_sha

def check_conditions(fs, branch_path, condition_check):
    """Check if conditions are met for processing"""
    if condition_check['type'] == 'file_exists':
        return fs.exists(f"lakefs://{branch_path}/{condition_check['path']}")
    
    elif condition_check['type'] == 'data_threshold':
        df = pd.read_csv(f"lakefs://{branch_path}/{condition_check['path']}")
        return len(df) >= condition_check['min_records']
    
    return False

# Usage
condition = {
    'type': 'data_threshold',
    'path': 'raw/new_data.csv',
    'min_records': 1000
}

updates = [
    {'type': 'merge', 'source': 'raw/new_data.csv', 'target': 'processed/all_data.csv'}
]

result = conditional_data_update("my-repo", "main", condition, updates)
```

## Best Practices

### Transaction Design Principles

1. **Keep Transactions Focused**: Group related operations together
2. **Handle Errors Gracefully**: Plan for partial failures
3. **Use Intermediate Commits**: For long-running processes
4. **Tag Important States**: Mark significant milestones
5. **Log Operations**: Maintain audit trails

### Performance Considerations
```python
# Good: Focused transaction
with fs.transaction("my-repo", "main") as tx:
    # Related operations only
    process_daily_sales(tx.branch.id)
    generate_daily_report(tx.branch.id)
    tx.commit("Daily sales processing")

# Avoid: Overly broad transaction
# with fs.transaction("my-repo", "main") as tx:
#     # Too many unrelated operations
#     process_sales()
#     train_ml_model()
#     generate_reports()
#     cleanup_old_data()
```

## Next Steps

- Review [filesystem operations](filesystem-api.md) for basic file handling
- Explore [data science integrations](integrations.md) for library-specific patterns
- Check the [lakefs-spec documentation](https://lakefs-spec.org/) for advanced features