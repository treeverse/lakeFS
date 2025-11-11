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

!!! info
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
    host="https://example.lakefs.io/api/v1",
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
    host="https://example.lakefs.io/api/v1",
    username=os.environ["LAKEFS_ACCESS_KEY"],
    password=os.environ["LAKEFS_SECRET_KEY"]
)

# JWT Bearer Token
configuration = lakefs_sdk.Configuration(
    host="https://example.lakefs.io/api/v1",
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
    host="https://example.lakefs.io/api/v1",
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
    host="https://example.lakefs.io/api/v1",
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
    host="https://example.lakefs.io/api/v1",
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
    host="https://example.lakefs.io/api/v1",
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
    host="https://example.lakefs.io/api/v1",
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

## Full API Reference

For complete API documentation and method signatures, see:

- **[Full SDK API Reference](https://pydocs-sdk.lakefs.io)** - Complete documentation for all API classes and methods
