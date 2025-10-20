---
title: Python - Generated SDK
description: Use the generated lakeFS SDK for direct API access with Python
---

# Using the Generated lakeFS SDK

The generated `lakefs-sdk` provides direct access to the lakeFS API based on the OpenAPI specification. This low-level SDK gives you complete control over API interactions and is useful when you need capabilities not covered by the High-Level SDK or when you prefer working directly with the API.

## When to Use

Use the generated SDK when you:

- Need **low-level API access** with full control over requests
- Want to use **features not available** in the High-Level SDK
- Prefer **direct API interaction** patterns
- Need to **access all API endpoints** programmatically
- Work with **API-first architectures**

For most common lakeFS operations (branches, tags, commits, objects), the **[High-Level SDK](./python.md)** is recommended as it provides a more Pythonic interface.

## Installation

Install the generated SDK using pip:

```shell
pip install lakefs-sdk
```

Or upgrade to the latest version:

```shell
pip install --upgrade lakefs-sdk
```

## Basic Setup

### Initializing the SDK

```python
import lakefs_sdk
from lakefs_sdk.rest import ApiException

# Configure the API client
configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000/api/v1",
    username="your-access-key",
    password="your-secret-key"
)

# Create an API client
api_client = lakefs_sdk.ApiClient(configuration)
```

### Authentication Options

The SDK supports multiple authentication methods:

```python
import lakefs_sdk
import os

# HTTP Basic Authentication
configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000/api/v1",
    username=os.environ["LAKEFS_ACCESS_KEY"],
    password=os.environ["LAKEFS_SECRET_KEY"]
)

# JWT Bearer Token
configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000/api/v1",
    access_token=os.environ["LAKEFS_TOKEN"]
)

api_client = lakefs_sdk.ApiClient(configuration)
```

## Simple Examples

### List Repositories

```python
import lakefs_sdk
from lakefs_sdk.apis.repositories_api import RepositoriesApi

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000/api/v1",
    username="your-access-key",
    password="your-secret-key"
)

with lakefs_sdk.ApiClient(configuration) as api_client:
    api_instance = RepositoriesApi(api_client)
    
    try:
        response = api_instance.list_repositories()
        for repo in response.results:
            print(f"Repository: {repo.id}")
    except lakefs_sdk.ApiException as e:
        print(f"Error: {e}")
```

### Get Repository Details

```python
import lakefs_sdk
from lakefs_sdk.apis.repositories_api import RepositoriesApi

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000/api/v1",
    username="your-access-key",
    password="your-secret-key"
)

with lakefs_sdk.ApiClient(configuration) as api_client:
    api_instance = RepositoriesApi(api_client)
    repository = "my-repo"
    
    try:
        repo = api_instance.get_repository(repository)
        print(f"Repository: {repo.id}")
        print(f"Storage: {repo.storage_namespace}")
        print(f"Created: {repo.creation_date}")
    except lakefs_sdk.ApiException as e:
        print(f"Error: {e}")
```

### List Branches

```python
import lakefs_sdk
from lakefs_sdk.apis.branches_api import BranchesApi

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000/api/v1",
    username="your-access-key",
    password="your-secret-key"
)

with lakefs_sdk.ApiClient(configuration) as api_client:
    api_instance = BranchesApi(api_client)
    repository = "my-repo"
    
    try:
        response = api_instance.list_branches(repository)
        for branch in response.results:
            print(f"Branch: {branch.id}")
    except lakefs_sdk.ApiException as e:
        print(f"Error: {e}")
```

### Create a Commit

```python
import lakefs_sdk
from lakefs_sdk.apis.commits_api import CommitsApi
from lakefs_sdk.models.commit_creation import CommitCreation

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000/api/v1",
    username="your-access-key",
    password="your-secret-key"
)

with lakefs_sdk.ApiClient(configuration) as api_client:
    api_instance = CommitsApi(api_client)
    repository = "my-repo"
    branch = "main"
    
    commit_creation = CommitCreation(
        message="My commit message",
        metadata={"author": "data-team"}
    )
    
    try:
        commit = api_instance.commit(repository, branch, commit_creation)
        print(f"Committed: {commit.id}")
    except lakefs_sdk.ApiException as e:
        print(f"Error: {e}")
```

## Error Handling

Handle API errors using `ApiException`:

```python
import lakefs_sdk
from lakefs_sdk.apis.repositories_api import RepositoriesApi
from lakefs_sdk.rest import ApiException

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000/api/v1",
    username="your-access-key",
    password="your-secret-key"
)

with lakefs_sdk.ApiClient(configuration) as api_client:
    api_instance = RepositoriesApi(api_client)
    
    try:
        response = api_instance.list_repositories()
    except ApiException as e:
        if e.status == 401:
            print("Unauthorized - check credentials")
        elif e.status == 404:
            print("Resource not found")
        else:
            print(f"API Error: {e.reason}")
```

## Available APIs

The generated SDK includes APIs for:

- **Repositories** - Create, delete, list repositories
- **Branches** - Create, delete, merge, diff branches
- **Commits** - Create, get, list commits
- **Tags** - Create, delete, list tags
- **Objects** - Upload, download, delete objects
- **References** - Work with references and commits
- **Auth** - User and group management
- **Actions** - Manage actions and hooks
- And many more...

## Full API Reference

For complete API documentation and method signatures, see:

- **[Full SDK API Reference](https://pydocs-sdk.lakefs.io)** - Complete documentation for all API classes and methods
- **[OpenAPI Specification](https://docs.lakefs.io/reference/api.md)** - Detailed API specification

## Comparison with High-Level SDK

| Aspect | Generated SDK | High-Level SDK |
|--------|---------------|----------------|
| Abstraction Level | Low (direct API) | High (Pythonic) |
| Ease of Use | Medium | Easy |
| Learning Curve | Steeper | Gentle |
| Feature Coverage | Complete | Most common operations |
| Code Verbosity | More | Less |
| Use Case | Advanced/API-first | Most users |

For most use cases, start with the [High-Level SDK](./python.md). Use the generated SDK when you need advanced features or direct API control.

## Best Practices

- **Use context managers** - Always use `with` statements for API clients to ensure proper cleanup
- **Handle exceptions** - Always catch `ApiException` to handle API errors gracefully
- **Configure properly** - Set the correct host, credentials, and optional parameters
- **Use pagination** - For large result sets, implement pagination using response pagination tokens
- **Refer to documentation** - Check the [full API reference](https://pydocs-sdk.lakefs.io) for method signatures
