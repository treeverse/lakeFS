---
title: Getting Started with Python and lakeFS
description: Complete installation and setup guide for all Python SDK options
sdk_types: ["high-level", "generated", "lakefs-spec", "boto3"]
difficulty: "beginner"
use_cases: ["getting-started", "installation", "authentication"]
topics: ["setup", "configuration", "credentials"]
audience: ["data-engineers", "data-scientists", "developers"]
last_updated: "2024-01-15"
---

# Getting Started with Python and lakeFS

This comprehensive guide walks you through installing and configuring Python SDKs for lakeFS. Follow the steps for your chosen SDK to get up and running quickly.

## Prerequisites

Before you begin, ensure you have:

- **Python 3.8 or higher** (check with `python --version`)
- **pip** package manager
- **Access to a lakeFS instance** (local or remote)
- **lakeFS credentials** (access key ID and secret access key)

## Quick SDK Selection

Not sure which SDK to choose? See our [SDK comparison](index.md#comprehensive-sdk-comparison) or use the [decision matrix](index.md#sdk-selection-decision-matrix).

| SDK | Installation | Best For |
|-----|-------------|----------|
| [High-Level SDK](#high-level-sdk-installation) | `pip install lakefs` | Most users, data pipelines |
| [Generated SDK](#generated-sdk-installation) | `pip install lakefs-sdk` | Direct API access |
| [lakefs-spec](#lakefs-spec-installation) | `pip install lakefs-spec` | Data science workflows |
| [Boto3](#boto3-installation) | `pip install boto3` | S3 migration |

## Installation Guide

### High-Level SDK Installation

The High-Level SDK provides the most user-friendly interface for lakeFS operations.

#### Basic Installation
```bash
pip install lakefs
```

#### Development Installation
For the latest features and bug fixes:
```bash
pip install --upgrade lakefs
```

#### Virtual Environment (Recommended)
```bash
# Create virtual environment
python -m venv lakefs-env
source lakefs-env/bin/activate  # On Windows: lakefs-env\Scripts\activate

# Install SDK
pip install lakefs
```

#### Verify Installation
```python
import lakefs
print(lakefs.__version__)
```

### Generated SDK Installation

The Generated SDK provides direct access to all lakeFS API endpoints.

#### Basic Installation
```bash
pip install lakefs-sdk
```

#### With Optional Dependencies
```bash
# For async support (if available)
pip install lakefs-sdk[async]
```

#### Verify Installation
```python
import lakefs_sdk
print(lakefs_sdk.__version__)
```

### lakefs-spec Installation

lakefs-spec provides filesystem-like operations and integrates with the fsspec ecosystem.

#### Basic Installation
```bash
pip install lakefs-spec
```

#### With Data Science Dependencies
```bash
# For pandas integration
pip install lakefs-spec[pandas]

# For complete data science stack
pip install lakefs-spec[all]
```

#### Verify Installation
```python
import lakefs_spec
print(lakefs_spec.__version__)
```

### Boto3 Installation

Use Boto3 for S3-compatible operations with lakeFS.

#### Basic Installation
```bash
pip install boto3
```

#### With Additional AWS Tools
```bash
# For AWS CLI compatibility
pip install boto3 awscli

# For async operations
pip install aioboto3
```

#### Verify Installation
```python
import boto3
print(boto3.__version__)
```

### Installation Troubleshooting

#### Common Issues

**Permission Errors:**
```bash
# Use --user flag to install for current user only
pip install --user lakefs

# Or use virtual environment (recommended)
python -m venv venv && source venv/bin/activate
```

**Version Conflicts:**
```bash
# Check for conflicts
pip check

# Upgrade pip first
pip install --upgrade pip

# Force reinstall
pip install --force-reinstall lakefs
```

**Network Issues:**
```bash
# Use different index
pip install -i https://pypi.org/simple/ lakefs

# Install from wheel
pip install --only-binary=all lakefs
```

## Authentication and Configuration

All Python SDKs support multiple authentication methods. Choose the method that best fits your deployment and security requirements.

### Method 1: Environment Variables (Recommended for Development)

Set environment variables in your shell or deployment environment:

#### Linux/macOS
```bash
export LAKEFS_ENDPOINT=http://localhost:8000
export LAKEFS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export LAKEFS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

#### Windows Command Prompt
```cmd
set LAKEFS_ENDPOINT=http://localhost:8000
set LAKEFS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
set LAKEFS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

#### Windows PowerShell
```powershell
$env:LAKEFS_ENDPOINT="http://localhost:8000"
$env:LAKEFS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
$env:LAKEFS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

#### Using .env Files
Create a `.env` file in your project directory:
```bash
LAKEFS_ENDPOINT=http://localhost:8000
LAKEFS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
LAKEFS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Load with python-dotenv:
```python
from dotenv import load_dotenv
load_dotenv()

import lakefs
# SDK will automatically use environment variables
```

### Method 2: Configuration File (Recommended for Production)

#### lakectl Configuration File
Create `~/.lakectl.yaml` (compatible with lakectl CLI):
```yaml
credentials:
  access_key_id: AKIAIOSFODNN7EXAMPLE
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
server:
  endpoint_url: http://localhost:8000
```

#### Custom Configuration File
Create a custom YAML configuration file:
```yaml
# config/lakefs.yaml
lakefs:
  endpoint: http://localhost:8000
  access_key_id: AKIAIOSFODNN7EXAMPLE
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  verify_ssl: true
```

Load in Python:
```python
import yaml
import lakefs

with open('config/lakefs.yaml', 'r') as f:
    config = yaml.safe_load(f)['lakefs']

client = lakefs.Client(
    host=config['endpoint'],
    username=config['access_key_id'],
    password=config['secret_access_key'],
    verify_ssl=config.get('verify_ssl', True)
)
```

### Method 3: Programmatic Configuration

#### High-Level SDK
```python
import lakefs

# Basic configuration
client = lakefs.Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

# Advanced configuration
client = lakefs.Client(
    host="https://lakefs.example.com",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    verify_ssl=True,
    ssl_ca_cert="/path/to/ca-bundle.pem",
    proxy="http://proxy.example.com:8080"
)

# Use client with repository operations
repo = lakefs.Repository("my-repo", client=client)
```

#### Generated SDK
```python
import lakefs_sdk
from lakefs_sdk.configuration import Configuration
from lakefs_sdk.api_client import ApiClient

# Configure client
configuration = Configuration(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

# Create API client
api_client = ApiClient(configuration)
```

#### lakefs-spec
```python
from lakefs_spec import LakeFSFileSystem

# Using credentials directly
fs = LakeFSFileSystem(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

# Auto-discover from ~/.lakectl.yaml
fs = LakeFSFileSystem()
```

#### Boto3
```python
import boto3
from botocore.config import Config

# Basic S3 client configuration
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# Advanced configuration with SSL and checksums
config = Config(
    request_checksum_calculation='when_required',
    response_checksum_validation='when_required',
    retries={'max_attempts': 3}
)

s3_client = boto3.client(
    's3',
    endpoint_url='https://lakefs.example.com',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    config=config
)
```

### SSL/TLS Configuration

#### Self-Signed Certificates (Development Only)
```python
import lakefs

# Disable SSL verification (NOT for production)
client = lakefs.Client(
    host="https://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    verify_ssl=False
)
```

!!! warning "Security Warning"
    Disabling SSL verification allows man-in-the-middle attacks. Never use `verify_ssl=False` in production environments.

#### Custom CA Certificates
```python
import lakefs

# Use custom CA bundle
client = lakefs.Client(
    host="https://lakefs.example.com",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    ssl_ca_cert="/path/to/ca-bundle.pem"
)
```

### Proxy Configuration

#### HTTP/HTTPS Proxy
```python
import lakefs

client = lakefs.Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    proxy="http://proxy.example.com:8080"
)
```

#### Proxy with Authentication
```python
import lakefs

client = lakefs.Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    proxy="http://user:pass@proxy.example.com:8080"
)
```

### Testing Your Configuration

#### Quick Connection Test
```python
import lakefs

try:
    # Test with High-Level SDK
    repos = list(lakefs.repositories())
    print(f"✅ Connected successfully! Found {len(repos)} repositories.")
except Exception as e:
    print(f"❌ Connection failed: {e}")
```

#### Comprehensive Health Check
```python
import lakefs

def test_lakefs_connection():
    try:
        # Test repository listing
        repos = list(lakefs.repositories())
        print(f"✅ Repository access: {len(repos)} repositories found")
        
        if repos:
            # Test branch listing on first repository
            repo = repos[0]
            branches = list(repo.branches())
            print(f"✅ Branch access: {len(branches)} branches in '{repo.id}'")
            
        return True
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

# Run the test
if test_lakefs_connection():
    print("🎉 lakeFS connection is working correctly!")
```

### Environment-Specific Configuration

#### Development Environment
```python
# development.py
import lakefs

# Use local lakeFS instance with relaxed SSL
client = lakefs.Client(
    host="http://localhost:8000",
    username="lakefs",
    password="lakefs_password",
    verify_ssl=False  # OK for local development
)
```

#### Production Environment
```python
# production.py
import os
import lakefs

# Use environment variables with strict SSL
client = lakefs.Client(
    host=os.getenv("LAKEFS_ENDPOINT"),
    username=os.getenv("LAKEFS_ACCESS_KEY_ID"),
    password=os.getenv("LAKEFS_SECRET_ACCESS_KEY"),
    verify_ssl=True,
    ssl_ca_cert=os.getenv("LAKEFS_CA_CERT_PATH")
)
```

## Next Steps

- **High-Level SDK:** Start with the [quickstart guide](high-level-sdk/quickstart.md)
- **Generated SDK:** See [API reference](generated-sdk/api-reference.md)
- **lakefs-spec:** Check [filesystem operations](lakefs-spec/filesystem-api.md)
- **Boto3:** Review [configuration guide](boto3/configuration.md)

## See Also

**SDK Selection:**
- [Python SDK Overview](index.md) - Compare all available Python SDK options
- [SDK Decision Matrix](index.md#sdk-selection-decision-matrix) - Choose the right SDK for your use case
- [Feature Comparison](reference/api-comparison.md) - Detailed feature comparison across SDKs

**SDK-Specific Getting Started:**
- [High-Level SDK Quickstart](high-level-sdk/quickstart.md) - Basic operations with simplified interface
- [Generated SDK Examples](generated-sdk/examples.md) - Direct API access patterns
- [lakefs-spec Filesystem API](lakefs-spec/filesystem-api.md) - File-like operations
- [Boto3 Configuration](boto3/configuration.md) - S3-compatible setup

**Authentication and Security:**
- [Best Practices Guide](reference/best-practices.md#security) - Production security recommendations
- [Troubleshooting Authentication](reference/troubleshooting.md#authentication-issues) - Common auth problems
- [SSL/TLS Configuration](reference/best-practices.md#ssl-configuration) - Secure connections

**Learning Resources:**
- [Data Science Workflow Tutorial](tutorials/data-science-workflow.md) - End-to-end data analysis
- [ETL Pipeline Tutorial](tutorials/etl-pipeline.md) - Building data pipelines
- [ML Experiment Tracking](tutorials/ml-experiment-tracking.md) - Model versioning workflow

**Reference Materials:**
- [Environment Configuration Examples](reference/best-practices.md#environment-configuration) - Production setup patterns
- [Connection Testing](reference/troubleshooting.md#connection-testing) - Verify your setup
- [Performance Optimization](reference/best-practices.md#performance) - Optimize SDK performance

**External Resources:**
- [lakeFS Documentation](https://docs.lakefs.io){:target="_blank"} - Complete lakeFS documentation
- [Python Package Index](https://pypi.org/search/?q=lakefs){:target="_blank"} - All lakeFS Python packages