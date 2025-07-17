---
title: Data Science Library Integrations
description: Using lakefs-spec with pandas, dask, and other data science libraries
sdk_types: ["lakefs-spec"]
difficulty: "beginner"
use_cases: ["data-science", "pandas-integration", "dask-integration", "jupyter-notebooks"]
topics: ["integrations", "pandas", "dask", "data-science", "libraries"]
audience: ["data-scientists", "analysts", "python-developers"]
last_updated: "2024-01-15"
---

# Data Science Library Integrations

lakefs-spec seamlessly integrates with popular data science libraries through the fsspec ecosystem, enabling direct data access with minimal code changes.

## Pandas Integration

### Reading Data with Pandas

#### CSV Files
```python
import pandas as pd

# Read CSV directly from lakeFS
df = pd.read_csv('lakefs://my-repo/main/data/sales.csv')
print(df.head())

# Read with specific parameters
df = pd.read_csv(
    'lakefs://my-repo/main/data/sales.csv',
    parse_dates=['date'],
    index_col='id'
)
```

#### Parquet Files
```python
# Read Parquet files
df = pd.read_parquet('lakefs://my-repo/main/data/sales.parquet')

# Read specific columns
df = pd.read_parquet(
    'lakefs://my-repo/main/data/sales.parquet',
    columns=['date', 'amount', 'customer_id']
)

# Read with filters
df = pd.read_parquet(
    'lakefs://my-repo/main/data/sales.parquet',
    filters=[('amount', '>', 1000)]
)
```

#### JSON Files
```python
# Read JSON files
df = pd.read_json('lakefs://my-repo/main/data/events.json', lines=True)

# Read nested JSON
df = pd.json_normalize(
    pd.read_json('lakefs://my-repo/main/data/nested.json')['data']
)
```

### Writing Data with Pandas

#### Save DataFrames
```python
import pandas as pd

# Create sample data
df = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=100),
    'value': range(100),
    'category': ['A', 'B'] * 50
})

# Save as CSV
df.to_csv('lakefs://my-repo/main/processed/results.csv', index=False)

# Save as Parquet
df.to_parquet('lakefs://my-repo/main/processed/results.parquet', index=False)

# Save as JSON
df.to_json('lakefs://my-repo/main/processed/results.json', orient='records', lines=True)
```

#### Advanced Parquet Options
```python
# Save with compression
df.to_parquet(
    'lakefs://my-repo/main/data/compressed.parquet',
    compression='snappy',
    index=False
)

# Save with partitioning
df.to_parquet(
    'lakefs://my-repo/main/data/partitioned/',
    partition_cols=['category'],
    index=False
)
```

### Data Processing Workflows

#### ETL Pipeline Example
```python
import pandas as pd

def process_sales_data(input_path, output_path):
    """Complete ETL pipeline using pandas and lakefs-spec"""
    
    # Extract: Read raw data
    raw_df = pd.read_csv(input_path)
    
    # Transform: Clean and process data
    processed_df = raw_df.copy()
    processed_df['date'] = pd.to_datetime(processed_df['date'])
    processed_df['amount'] = processed_df['amount'].astype(float)
    processed_df = processed_df.dropna()
    
    # Add calculated columns
    processed_df['year'] = processed_df['date'].dt.year
    processed_df['month'] = processed_df['date'].dt.month
    processed_df['amount_category'] = pd.cut(
        processed_df['amount'], 
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['small', 'medium', 'large', 'xlarge']
    )
    
    # Load: Save processed data
    processed_df.to_parquet(output_path, index=False)
    
    return len(processed_df)

# Usage
records_processed = process_sales_data(
    'lakefs://my-repo/main/raw/sales.csv',
    'lakefs://my-repo/main/processed/sales_clean.parquet'
)
print(f"Processed {records_processed} records")
```

## Dask Integration

### Distributed Processing with Dask

#### Reading Large Datasets
```python
import dask.dataframe as dd

# Read large CSV files with Dask
df = dd.read_csv('lakefs://my-repo/main/data/large_dataset.csv')

# Read multiple files
df = dd.read_csv('lakefs://my-repo/main/data/*.csv')

# Read Parquet with Dask
df = dd.read_parquet('lakefs://my-repo/main/data/partitioned_data/')
```

#### Processing Large Datasets
```python
import dask.dataframe as dd

def process_large_dataset():
    """Process large datasets with Dask and lakefs-spec"""
    
    # Read large dataset
    df = dd.read_parquet('lakefs://my-repo/main/raw/large_data/')
    
    # Perform distributed operations
    result = (df
              .groupby('category')
              .amount.sum()
              .compute())  # Trigger computation
    
    # Save results
    result_df = pd.DataFrame({'category': result.index, 'total': result.values})
    result_df.to_csv('lakefs://my-repo/main/results/category_totals.csv', index=False)
    
    return result

# Usage
totals = process_large_dataset()
print(totals)
```

#### Dask with Custom Storage Options
```python
import dask.dataframe as dd

# Read with custom storage options (if needed)
df = dd.read_parquet(
    'lakefs://my-repo/main/data/dataset.parquet',
    storage_options={
        'host': 'http://localhost:8000',
        'username': 'access_key',
        'password': 'secret_key'
    }
)
```

## Other Data Science Libraries

### NumPy Integration
```python
import numpy as np
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# Save NumPy arrays
arr = np.random.rand(1000, 100)
with fs.open('lakefs://my-repo/main/arrays/data.npy', 'wb') as f:
    np.save(f, arr)

# Load NumPy arrays
with fs.open('lakefs://my-repo/main/arrays/data.npy', 'rb') as f:
    loaded_arr = np.load(f)

print(f"Array shape: {loaded_arr.shape}")
```

### Scikit-learn Integration
```python
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib
from lakefs_spec import LakeFSFileSystem

def ml_workflow_with_lakefs():
    """Complete ML workflow using lakefs-spec"""
    
    # Load training data
    df = pd.read_csv('lakefs://my-repo/main/data/training_data.csv')
    
    # Prepare features and target
    X = df.drop('target', axis=1)
    y = df['target']
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    
    # Train model
    model = RandomForestClassifier()
    model.fit(X_train, y_train)
    
    # Save model to lakeFS
    fs = LakeFSFileSystem()
    with fs.open('lakefs://my-repo/main/models/rf_model.pkl', 'wb') as f:
        joblib.dump(model, f)
    
    # Save test results
    predictions = model.predict(X_test)
    results_df = pd.DataFrame({
        'actual': y_test,
        'predicted': predictions
    })
    results_df.to_csv('lakefs://my-repo/main/results/test_results.csv', index=False)
    
    return model.score(X_test, y_test)

# Usage
accuracy = ml_workflow_with_lakefs()
print(f"Model accuracy: {accuracy:.3f}")
```

### Matplotlib/Seaborn Integration
```python
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from lakefs_spec import LakeFSFileSystem
import io

def create_and_save_plots():
    """Create plots and save them to lakeFS"""
    
    # Load data
    df = pd.read_csv('lakefs://my-repo/main/data/sales.csv')
    
    # Create plot
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='date', y='amount')
    plt.title('Sales Over Time')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save plot to lakeFS
    fs = LakeFSFileSystem()
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', dpi=300)
    img_buffer.seek(0)
    
    with fs.open('lakefs://my-repo/main/plots/sales_trend.png', 'wb') as f:
        f.write(img_buffer.getvalue())
    
    plt.close()

# Usage
create_and_save_plots()
```

## Advanced Integration Patterns

### Custom Data Loaders
```python
from lakefs_spec import LakeFSFileSystem
import pandas as pd

class LakeFSDataLoader:
    """Custom data loader for common data science tasks"""
    
    def __init__(self, repo, branch):
        self.fs = LakeFSFileSystem()
        self.base_path = f"lakefs://{repo}/{branch}"
    
    def load_dataset(self, dataset_name, file_format='parquet'):
        """Load dataset by name"""
        path = f"{self.base_path}/datasets/{dataset_name}.{file_format}"
        
        if file_format == 'parquet':
            return pd.read_parquet(path)
        elif file_format == 'csv':
            return pd.read_csv(path)
        elif file_format == 'json':
            return pd.read_json(path, lines=True)
        else:
            raise ValueError(f"Unsupported format: {file_format}")
    
    def save_dataset(self, df, dataset_name, file_format='parquet'):
        """Save dataset by name"""
        path = f"{self.base_path}/datasets/{dataset_name}.{file_format}"
        
        if file_format == 'parquet':
            df.to_parquet(path, index=False)
        elif file_format == 'csv':
            df.to_csv(path, index=False)
        elif file_format == 'json':
            df.to_json(path, orient='records', lines=True)
    
    def list_datasets(self):
        """List available datasets"""
        datasets_path = f"{self.base_path}/datasets/"
        files = self.fs.ls(datasets_path)
        return [f.split('/')[-1] for f in files if f.endswith(('.parquet', '.csv', '.json'))]

# Usage
loader = LakeFSDataLoader('my-repo', 'main')

# Load data
df = loader.load_dataset('sales_data')

# Process data
processed_df = df.groupby('category').sum().reset_index()

# Save results
loader.save_dataset(processed_df, 'sales_summary')

# List available datasets
datasets = loader.list_datasets()
print(f"Available datasets: {datasets}")
```

### Jupyter Notebook Integration
```python
# In Jupyter notebooks, you can use lakefs-spec seamlessly

import pandas as pd
import matplotlib.pyplot as plt

# Load and visualize data directly from lakeFS
df = pd.read_csv('lakefs://my-repo/main/data/experiment_results.csv')

# Create interactive plots
df.plot(x='date', y='metric', kind='line', figsize=(12, 6))
plt.title('Experiment Results Over Time')
plt.show()

# Save notebook outputs back to lakeFS
summary_stats = df.describe()
summary_stats.to_csv('lakefs://my-repo/main/analysis/summary_stats.csv')
```

### Multi-format Data Pipeline
```python
def multi_format_pipeline(repo, branch):
    """Pipeline that handles multiple data formats"""
    
    base_path = f"lakefs://{repo}/{branch}"
    
    # Read different formats
    csv_data = pd.read_csv(f"{base_path}/raw/data.csv")
    json_data = pd.read_json(f"{base_path}/raw/events.json", lines=True)
    parquet_data = pd.read_parquet(f"{base_path}/raw/metrics.parquet")
    
    # Combine and process
    combined_data = pd.concat([
        csv_data.assign(source='csv'),
        json_data.assign(source='json'),
        parquet_data.assign(source='parquet')
    ], ignore_index=True)
    
    # Save in different formats for different use cases
    # Fast analytics: Parquet
    combined_data.to_parquet(f"{base_path}/processed/combined.parquet", index=False)
    
    # Human readable: CSV
    summary = combined_data.groupby('source').size().reset_index(name='count')
    summary.to_csv(f"{base_path}/processed/summary.csv", index=False)
    
    # API consumption: JSON
    combined_data.head(100).to_json(
        f"{base_path}/processed/sample.json", 
        orient='records', 
        lines=True
    )
    
    return len(combined_data)

# Usage
total_records = multi_format_pipeline('my-repo', 'main')
print(f"Processed {total_records} total records")
```

## Performance Optimization

### Efficient Data Access Patterns
```python
# Use appropriate file formats for your use case
# Parquet for analytics workloads
df.to_parquet('lakefs://my-repo/main/data/analytics.parquet', 
              compression='snappy')

# CSV for human-readable data
df.to_csv('lakefs://my-repo/main/data/readable.csv', index=False)

# Use partitioning for large datasets
df.to_parquet('lakefs://my-repo/main/data/partitioned/',
              partition_cols=['year', 'month'],
              index=False)
```

### Caching Strategies
```python
# Cache frequently accessed data locally
import os

def cached_read(lakefs_path, local_cache_dir='./cache'):
    """Read with local caching"""
    os.makedirs(local_cache_dir, exist_ok=True)
    
    # Create cache filename
    cache_filename = lakefs_path.replace('/', '_').replace(':', '_')
    cache_path = os.path.join(local_cache_dir, cache_filename)
    
    # Check if cached version exists
    if os.path.exists(cache_path):
        return pd.read_parquet(cache_path)
    
    # Read from lakeFS and cache
    df = pd.read_parquet(lakefs_path)
    df.to_parquet(cache_path, index=False)
    
    return df

# Usage
df = cached_read('lakefs://my-repo/main/data/large_dataset.parquet')
```

## Next Steps

- Learn about [transaction patterns](transactions.md)
- Explore the [lakefs-spec documentation](https://lakefs-spec.org/)
- Check out [pandas documentation](https://pandas.pydata.org/) for more data manipulation techniques