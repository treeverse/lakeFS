---
title: Using lakefs-spec (fsspec compatible)
description: Learn how to use lakefs-spec for file-system-like operations on lakeFS, compatible with fsspec.
parent: Python Integration
grand_parent: Integrations
---

# Using `lakefs-spec` for Filesystem-like Operations

{% raw %}{% include toc.html %}{% endraw %}

## Overview

The [`lakefs-spec`](https://lakefs-spec.org/latest/) project provides an `fsspec`-compatible file system implementation for lakeFS. This allows you to interact with objects in your lakeFS repositories using familiar file system semantics (e.g., `open`, `ls`, `put`, `get`) and seamlessly integrate with popular Python libraries that build on `fsspec`, such as Pandas, Dask, DuckDB, and Polars.

{: .note }
> **Note:** `lakefs-spec` is a third-party package developed and maintained by the community, not by lakeFS (Treeverse). For issues, bug reports, or contributions related to `lakefs-spec`, please refer to the [official `lakefs-spec` GitHub repository](https://github.com/aai-institute/lakefs-spec).

### Key Features

*   **`fsspec` Compatibility**: Works out-of-the-box with libraries that expect an `fsspec.AbstractFileSystem` interface.
*   **Familiar API**: Provides methods like `open()`, `cat()`, `pipe()`, `cp_file()`, `ls()`, `rm()`, `exists()`, `isdir()`, `isfile()`, etc.
*   **Transactional Commits**: Supports atomic operations (commits, tags) on lakeFS branches within a transaction context.
*   **Caching**: Offers caching mechanisms to speed up repeated access to files.

### Installation

Install `lakefs-spec` directly using pip:

```shell
python -m pip install lakefs-spec
```

Or, to include caching dependencies:
```shell
python -m pip install lakefs-spec[cache]
```

### Configuration

`lakefs-spec` can be configured similarly to `lakectl`, typically by having a `~/.lakectl.yaml` file with your lakeFS server endpoint and credentials. It can also be configured programmatically or via environment variables.

Refer to the [`lakefs-spec` configuration documentation](https://lakefs-spec.org/latest/guides/configuration/) for details.

### Further Information & API Reference

For more detailed user guides, tutorials on integrations with other data science tools, and the complete API reference, please visit the **official `lakefs-spec` documentation**:
[https://lakefs-spec.org/latest/](https://lakefs-spec.org/latest/)

This includes:
*   [Full API Reference](https://lakefs-spec.org/latest/reference/lakefs_spec/)
*   [Guides on Integrations (Pandas, Polars, DuckDB etc.)](https://lakefs-spec.org/latest/guides/integrations/)
*   [Information on Caching and Configuration](https://lakefs-spec.org/latest/guides/caching/)

## Tutorials / User Guides

This section provides step-by-step examples for common tasks using `lakefs-spec`. Ensure your lakeFS instance is running and accessible, and that you have appropriate credentials configured (typically via `~/.lakectl.yaml`).

### Basic File Operations

Here's how you can write and read a file directly to/from a branch in a lakeFS repository:

```python
from pathlib import Path
from lakefs_spec import LakeFSFileSystem

# Ensure your lakeFS credentials and endpoint are configured (e.g., via ~/.lakectl.yaml)
fs = LakeFSFileSystem()

repo_name = "example-repo"  # Replace with your repository name, or create one
branch_name = "main"        # Replace with your branch name
local_file_path_obj = Path("demo_data_spec.txt") # Renamed to avoid conflict
lakefs_file_path = f"{repo_name}/{branch_name}/{local_file_path_obj.name}"

# 1. Create a local example file
local_file_path_obj.write_text("Hello, lakeFS world, using lakefs-spec!")
print(f"Created local file: '{local_file_path_obj}'")

# 2. Upload the file to lakeFS
# For this example to run, ensure 'example-repo' exists and 'main' branch exists.
try:
    fs.put_file(str(local_file_path_obj), lakefs_file_path) # Ensure local path is string
    print(f"Uploaded '{local_file_path_obj}' to '{lakefs_file_path}' on lakeFS.")
except Exception as e:
    print(f"Error uploading file: {e}")
    print(f"Please ensure repository '{repo_name}' and branch '{branch_name}' exist.")

# 3. Read the file back from lakeFS
try:
    if fs.exists(lakefs_file_path):
        with fs.open(lakefs_file_path, "rt") as f_remote: # Renamed
            content = f_remote.read()
            print(f"Content of '{lakefs_file_path}' from lakeFS:\n'{content}'")
    else:
        print(f"File '{lakefs_file_path}' does not exist on lakeFS.")
except Exception as e:
    print(f"Error reading file: {e}")

# 4. Clean up local file
local_file_path_obj.unlink(missing_ok=True)
print(f"Cleaned up local file: '{local_file_path_obj}'")
```

### Integration with Pandas

A common use case is reading and writing data with Pandas directly from/to lakeFS. `lakefs-spec` enables this by registering the `lakefs://` protocol with `fsspec`.

```python
import pandas as pd
# from lakefs_spec import LakeFSFileSystem # Often not needed explicitly for fsspec URLs

# Ensure lakeFS client is configured (e.g. ~/.lakectl.yaml)

repo_name = "quickstart" 
branch_name = "main"
# Note: The quickstart repo contains 'lakes.parquet' at the root of the 'main' branch.
source_parquet_uri = f"lakefs://{repo_name}/{branch_name}/lakes.parquet" # Renamed
output_csv_uri = f"lakefs://{repo_name}/{branch_name}/data/german_lakes_spec.csv" # Renamed

try:
    print(f"Reading Parquet file from: {source_parquet_uri}")
    df = pd.read_parquet(source_parquet_uri) 
    print("Successfully read Parquet into DataFrame:")
    print(df.head())

    german_lakes_df = df[df['Country'] == 'Germany']
    print("\nFiltered German lakes:")
    print(german_lakes_df.head())

    print(f"\nWriting filtered data to CSV at: {output_csv_uri}")
    german_lakes_df.to_csv(output_csv_uri, index=False)
    print("Successfully wrote CSV back to lakeFS.")

except FileNotFoundError:
    print(f"Error: The file '{source_parquet_uri}' was not found.")
    print("Please ensure the 'quickstart' repository is populated (e.g., 'lakectl quickstart' or UI setup).")
except Exception as e:
    print(f"An error occurred with Pandas integration: {e}")
```
**Note**: For the Pandas example to work, you need the `quickstart` repository populated or adapt paths to your data.

### Using Transactions for Atomic Operations

`lakefs-spec` supports transactions for committing changes atomically. This creates a temporary branch for operations, which is merged back to the base branch upon successful completion of the transaction block.

```python
from pathlib import Path
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem() 

repo_name = "example-repo" 
base_branch_name = "main"   

train_file = Path("train-data-spec.txt") # Renamed
test_file = Path("test-data-spec.txt")   # Renamed
train_file.write_text("Training data for lakefs-spec transaction.")
test_file.write_text("Test data for lakefs-spec transaction.")
print(f"Created local files: '{train_file}', '{test_file}'")

try:
    # Ensure base repository and branch exist before starting a transaction.
    # Example check (optional, adapt as needed):
    # if not fs.exists(f"{repo_name}/{base_branch_name}/"):
    #     raise FileNotFoundError(f"Base path '{repo_name}/{base_branch_name}' not found or not a directory.")

    with fs.transaction(repository=repo_name, base_branch=base_branch_name) as tx:
        print(f"Transaction started on repo '{repo_name}', base branch '{base_branch_name}'. Temp branch: '{tx.branch.id}'")

        # Destination paths on lakeFS within the transaction branch
        train_dest_path = f"{repo_name}/{tx.branch.id}/{train_file.name}"
        test_dest_path = f"{repo_name}/{tx.branch.id}/{test_file.name}"

        fs.put_file(str(train_file), train_dest_path)
        print(f"Uploaded '{train_file.name}' to transaction branch.")
        commit1 = tx.commit(message="Add training data (spec tx)")
        print(f"Committed training data. SHA: {commit1.id}")

        fs.put_file(str(test_file), test_dest_path)
        print(f"Uploaded '{test_file.name}' to transaction branch.")
        commit2 = tx.commit(message="Add test data (spec tx)", metadata={"version": "1.0_spec_tx"})
        print(f"Committed test data. SHA: {commit2.id}")
        
        tag_name = "v1.0_spec_data"
        tx.tag(tag_name=tag_name) # Tags the last commit in the transaction
        print(f"Tagged final commit in transaction with '{tag_name}'.")

    print(f"Transaction completed and merged into '{base_branch_name}'.")

except Exception as e:
    print(f"Transaction error: {e}")
    print("If error occurred within 'with fs.transaction()', it was aborted; no changes merged.")
    print(f"Ensure repository '{repo_name}' and branch '{base_branch_name}' exist.")
finally:
    train_file.unlink(missing_ok=True)
    test_file.unlink(missing_ok=True)
    print(f"Cleaned up local files: '{train_file}', '{test_file}'")
```

### Error Handling Notes

When using `lakefs-spec`, operations might raise standard Python exceptions like `FileNotFoundError` if a path doesn't exist, or `PermissionError` for access issues. More general issues during communication with the lakeFS server might result in an `Exception` that wraps the underlying error from the lakeFS client or HTTP request.

Always consult the [official `lakefs-spec` error handling documentation](https://lakefs-spec.org/latest/guides/exceptions/) and use `try...except` blocks for robust scripting.

## Best Practices and Performance

For guidance on optimizing your use of Python with lakeFS, choosing the right tools for different scenarios, and general performance considerations (including `lakefs-spec` caching), please refer to our comprehensive guide:
[Python SDKs: Best Practices & Performance](./best_practices.md)

---
