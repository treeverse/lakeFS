---
title: Generated Python SDK
description: Direct API access using the Generated Python SDK
sdk_types: ["generated"]
difficulty: "intermediate"
use_cases: ["direct-api", "custom-operations", "advanced-integration"]
topics: ["api-access", "openapi", "low-level"]
audience: ["developers", "advanced-users", "integrators"]
last_updated: "2024-01-15"
---

# Generated Python SDK

The Generated Python SDK provides direct access to the lakeFS API based on the OpenAPI specification. It offers complete API coverage and is the foundation for the High-Level SDK.

## Overview

The Generated SDK is automatically generated from lakeFS's OpenAPI specification, ensuring it stays up-to-date with all API features. It provides low-level access to every lakeFS API endpoint with full type safety and validation.

### Key Features

- **Complete API Coverage** - Access to all lakeFS API endpoints
- **OpenAPI-Based** - Generated from the official lakeFS OpenAPI specification  
- **Type Safety** - Full type hints and model validation
- **Direct Control** - Low-level access for custom operations
- **Synchronous Operations** - Straightforward request/response patterns

### Relationship to High-Level SDK

The High-Level SDK is built on top of the Generated SDK, providing:
- Simplified interfaces for common operations
- Advanced features like transactions and streaming I/O
- Pythonic abstractions over raw API calls

You can access the Generated SDK client directly from High-Level SDK objects when needed.

## When to Use Generated SDK

Choose the Generated SDK when you need:

### Custom Operations
- Operations not available in High-Level SDK
- Access to experimental or internal APIs
- Fine-grained control over API parameters

### Performance Critical Applications
- Direct API access for optimal performance
- Minimal overhead for high-throughput scenarios
- Custom retry and error handling logic

### Advanced Integration Development
- Building custom integrations or tools
- Implementing complex workflows
- Need for all API parameters and options

### Migration from REST API
- Converting existing REST API calls to Python
- Maintaining compatibility with existing code patterns
- Direct mapping from API documentation

## Installation and Setup

### Installation

Install the Generated SDK using pip:

```bash
pip install lakefs-sdk
```

### Basic Configuration

The Generated SDK uses a configuration object to manage connection settings:

```python
import lakefs_sdk

# Create configuration
configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE", 
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

# Create API client
with lakefs_sdk.ApiClient(configuration) as api_client:
    # Use the client with API classes
    repositories_api = lakefs_sdk.RepositoriesApi(api_client)
    repos = repositories_api.list_repositories()
```

### Authentication Methods

#### Basic Authentication (Username/Password)

```python
import lakefs_sdk

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="your-access-key-id",
    password="your-secret-access-key"
)
```

#### Bearer Token Authentication

```python
import lakefs_sdk

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    access_token="your-jwt-token"
)
```

#### API Key Authentication

```python
import lakefs_sdk

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000"
)

# Set API key for specific auth method
configuration.api_key['cookie_auth'] = 'your-api-key'
configuration.api_key_prefix['cookie_auth'] = 'Bearer'  # Optional prefix
```

#### Environment Variables

You can also configure authentication using environment variables:

```python
import os
import lakefs_sdk

configuration = lakefs_sdk.Configuration(
    host=os.environ.get("LAKEFS_HOST", "http://localhost:8000"),
    username=os.environ.get("LAKEFS_ACCESS_KEY_ID"),
    password=os.environ.get("LAKEFS_SECRET_ACCESS_KEY")
)
```

### Advanced Configuration Options

#### SSL and TLS Settings

```python
import lakefs_sdk

configuration = lakefs_sdk.Configuration(
    host="https://your-lakefs-instance.com",
    username="your-access-key-id",
    password="your-secret-access-key"
)

# SSL verification (default: True)
configuration.verify_ssl = True

# Custom SSL certificate path
configuration.ssl_ca_cert = "/path/to/ca-cert.pem"

# Client certificate authentication
configuration.cert_file = "/path/to/client-cert.pem"
configuration.key_file = "/path/to/client-key.pem"
```

#### Proxy Configuration

```python
import lakefs_sdk

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="your-access-key-id",
    password="your-secret-access-key"
)

# HTTP proxy
configuration.proxy = "http://proxy.example.com:8080"

# HTTPS proxy  
configuration.proxy_headers = {
    'Proxy-Authorization': 'Basic dXNlcjpwYXNz'
}
```

#### Timeout and Retry Settings

```python
import lakefs_sdk

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="your-access-key-id",
    password="your-secret-access-key"
)

# Connection timeout (seconds)
configuration.connection_pool_maxsize = 10

# Custom user agent
configuration.user_agent = "MyApp/1.0 lakefs-sdk"
```

## Basic Usage Pattern

### Standard API Client Usage

```python
import lakefs_sdk
from lakefs_sdk.rest import ApiException

# Configure client
configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

# Use context manager for proper resource cleanup
with lakefs_sdk.ApiClient(configuration) as api_client:
    try:
        # Create API instance
        repositories_api = lakefs_sdk.RepositoriesApi(api_client)
        
        # Make API call
        repositories = repositories_api.list_repositories()
        
        # Process results
        for repo in repositories.results:
            print(f"Repository: {repo.id}")
            
    except ApiException as e:
        print(f"API Error: {e.status} - {e.reason}")
        print(f"Response body: {e.body}")
```

### Error Handling

The Generated SDK raises `ApiException` for API errors:

```python
import lakefs_sdk
from lakefs_sdk.rest import ApiException

with lakefs_sdk.ApiClient(configuration) as api_client:
    repositories_api = lakefs_sdk.RepositoriesApi(api_client)
    
    try:
        repo = repositories_api.get_repository("nonexistent-repo")
    except ApiException as e:
        if e.status == 404:
            print("Repository not found")
        elif e.status == 401:
            print("Authentication failed")
        elif e.status == 403:
            print("Access denied")
        else:
            print(f"API error: {e.status} - {e.reason}")
```

## Quick Start Example

Here's a complete example showing common operations:

```python
import lakefs_sdk
from lakefs_sdk.rest import ApiException

# Configuration
configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

with lakefs_sdk.ApiClient(configuration) as api_client:
    try:
        # Initialize API classes
        repositories_api = lakefs_sdk.RepositoriesApi(api_client)
        branches_api = lakefs_sdk.BranchesApi(api_client)
        objects_api = lakefs_sdk.ObjectsApi(api_client)
        
        # List repositories
        print("Repositories:")
        repos = repositories_api.list_repositories()
        for repo in repos.results:
            print(f"  - {repo.id}")
        
        # Get repository details
        if repos.results:
            repo_id = repos.results[0].id
            repo = repositories_api.get_repository(repo_id)
            print(f"\nRepository '{repo_id}' default branch: {repo.default_branch}")
            
            # List branches
            branches = branches_api.list_branches(repo_id)
            print(f"Branches in {repo_id}:")
            for branch in branches.results:
                print(f"  - {branch.id} (commit: {branch.commit_id})")
            
            # List objects in main branch
            if branches.results:
                branch_id = branches.results[0].id
                objects = objects_api.list_objects(repo_id, branch_id)
                print(f"\nObjects in {repo_id}/{branch_id}:")
                for obj in objects.results:
                    print(f"  - {obj.path} ({obj.size_bytes} bytes)")
                    
    except ApiException as e:
        print(f"Error: {e.status} - {e.reason}")
```

**Expected Output:**
```
Repositories:
  - my-repo
  - data-lake

Repository 'my-repo' default branch: main

Branches in my-repo:
  - main (commit: c7a632d0a7c4c9b5e8f1a2b3c4d5e6f7g8h9i0j1)
  - feature-branch (commit: d8b743e1b8d5d0c6f9g2b4c5d6e7f8g9h0i1j2k3)

Objects in my-repo/main:
  - data/users.csv (1024 bytes)
  - models/model.pkl (2048 bytes)
```

## Documentation Sections

- **[API Reference](api-reference.md)** - Complete API classes and methods
- **[Usage Examples](examples.md)** - Common usage patterns and operations
- **[Direct Access](direct-access.md)** - Accessing Generated SDK from High-Level SDK

## Next Steps

- Review the [API reference](api-reference.md) for complete documentation
- See [usage examples](examples.md) for common patterns  
- Learn about [direct access](direct-access.md) from High-Level SDK
- Check the [troubleshooting guide](../reference/troubleshooting.md) for common issues

## See Also

**SDK Selection and Comparison:**
- [Python SDK Overview](../index.md) - Compare all Python SDK options
- [SDK Decision Matrix](../index.md#sdk-selection-decision-matrix) - Choose the right SDK
- [API Feature Comparison](../reference/api-comparison.md) - Detailed feature comparison

**Generated SDK Documentation:**
- [API Reference](api-reference.md) - Complete API classes and methods documentation
- [Usage Examples](examples.md) - Common patterns and operations
- [Direct Access from High-Level SDK](direct-access.md) - Using Generated SDK with High-Level SDK

**Alternative SDK Options:**
- [High-Level SDK](../high-level-sdk/index.md) - Simplified Python interface built on Generated SDK
- [High-Level SDK Quickstart](../high-level-sdk/quickstart.md) - Easy-to-use interface for common operations
- [lakefs-spec](../lakefs-spec/index.md) - Filesystem interface for data science workflows
- [Boto3 Integration](../boto3/index.md) - S3-compatible operations

**Setup and Configuration:**
- [Installation Guide](../getting-started.md) - Complete setup instructions for all SDKs
- [Authentication Methods](../getting-started.md#authentication-and-configuration) - All credential options
- [Best Practices](../reference/best-practices.md#configuration) - Production configuration guidance

**Learning Resources:**
- [Data Science Tutorial](../tutorials/data-science-workflow.md) - End-to-end workflow examples
- [ETL Pipeline Tutorial](../tutorials/etl-pipeline.md) - Building data pipelines
- [Advanced Integration Patterns](../reference/best-practices.md#integration-patterns) - Custom tooling development

**Reference Materials:**
- [Error Handling Guide](../reference/troubleshooting.md#error-handling) - Exception handling strategies
- [Performance Optimization](../reference/best-practices.md#performance) - Optimize API usage
- [Security Best Practices](../reference/best-practices.md#security) - Secure API access patterns

**External Resources:**
- [Generated SDK API Documentation](https://pydocs-sdk.lakefs.io){:target="_blank"} - Auto-generated API reference
- [lakeFS OpenAPI Specification](https://docs.lakefs.io/reference/api.html){:target="_blank"} - Complete API specification
- [lakeFS REST API Documentation](https://docs.lakefs.io/reference/api.html){:target="_blank"} - REST API reference