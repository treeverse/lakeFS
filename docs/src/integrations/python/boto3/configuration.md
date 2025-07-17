---
title: Boto3 Configuration
description: Comprehensive setup and configuration guide for using Boto3 with lakeFS
sdk_types: ["boto3"]
difficulty: "beginner"
use_cases: ["s3-migration", "legacy-integration"]
---

# Boto3 Configuration

This guide covers comprehensive setup and configuration options for using Boto3 with lakeFS, including SSL settings, proxy configuration, and checksum handling for newer Boto3 versions.

## Prerequisites

Before configuring Boto3 with lakeFS, ensure you have:

- **lakeFS Server** - Running lakeFS instance (local or remote)
- **Access Credentials** - lakeFS access key ID and secret access key
- **Boto3 Installed** - `pip install boto3`
- **Network Access** - Connectivity to lakeFS endpoint

## Basic Configuration

### Minimal Setup

The simplest way to configure Boto3 with lakeFS:

```python
import boto3

# Basic lakeFS configuration
s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000',  # lakeFS endpoint
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# Test the connection
try:
    response = s3_client.list_buckets()
    print(f"Connected! Found {len(response['Buckets'])} repositories")
except Exception as e:
    print(f"Connection failed: {e}")
```

**Expected Output:**
```
Connected! Found 3 repositories
```

### Environment-Based Configuration

For production environments, use environment variables:

```python
import boto3
import os

# Set environment variables first
os.environ['LAKEFS_ENDPOINT'] = 'https://lakefs.example.com'
os.environ['LAKEFS_ACCESS_KEY_ID'] = 'AKIAIOSFODNN7EXAMPLE'
os.environ['LAKEFS_SECRET_ACCESS_KEY'] = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

# Production configuration with environment variables
s3_client = boto3.client('s3',
    endpoint_url=os.getenv('LAKEFS_ENDPOINT'),
    aws_access_key_id=os.getenv('LAKEFS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('LAKEFS_SECRET_ACCESS_KEY')
)
```

### Configuration Validation

Always validate your configuration:

```python
def validate_lakefs_config(s3_client):
    """Validate lakeFS Boto3 configuration"""
    try:
        # Test basic connectivity
        response = s3_client.list_buckets()
        print("✓ Connection successful")
        
        # Test repository access
        repos = response.get('Buckets', [])
        print(f"✓ Found {len(repos)} repositories")
        
        if repos:
            # Test object operations on first repository
            test_repo = repos[0]['Name']
            try:
                s3_client.list_objects_v2(Bucket=test_repo, MaxKeys=1)
                print(f"✓ Repository access confirmed: {test_repo}")
            except Exception as e:
                print(f"⚠ Repository access limited: {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration invalid: {e}")
        return False

# Usage
if validate_lakefs_config(s3_client):
    print("Configuration is ready for use!")
```

## Advanced Configuration Options

### SSL and Security Settings
```python
import boto3

# HTTPS configuration
s3_client = boto3.client('s3',
    endpoint_url='https://lakefs.example.com',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    verify=True,  # Verify SSL certificates
    use_ssl=True
)

# Custom CA certificate
s3_client = boto3.client('s3',
    endpoint_url='https://lakefs.example.com',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    verify='/path/to/ca-bundle.pem'
)

# Disable SSL verification (development only)
s3_client = boto3.client('s3',
    endpoint_url='https://lakefs.example.com',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    verify=False
)
```

### Checksum Configuration

For newer versions of Boto3, configure checksum settings to avoid issues:

```python
import boto3
from botocore.config import Config

# Configure checksum settings for newer Boto3 versions
config = Config(
    request_checksum_calculation='when_required',
    response_checksum_validation='when_required'
)

s3_client = boto3.client('s3',
    endpoint_url='https://lakefs.example.com',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    config=config
)
```

### Proxy Configuration
```python
import boto3
from botocore.config import Config

# Configure proxy settings
config = Config(
    proxies={
        'http': 'http://proxy.example.com:8080',
        'https': 'https://proxy.example.com:8080'
    }
)

s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    config=config
)
```

## Connection and Performance Settings

### Timeout Configuration
```python
import boto3
from botocore.config import Config

# Configure timeouts
config = Config(
    connect_timeout=10,  # Connection timeout in seconds
    read_timeout=30,     # Read timeout in seconds
    retries={
        'max_attempts': 3,
        'mode': 'adaptive'
    }
)

s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    config=config
)
```

### Connection Pooling
```python
import boto3
from botocore.config import Config

# Configure connection pooling
config = Config(
    max_pool_connections=50,  # Maximum connections in pool
    retries={
        'max_attempts': 3,
        'mode': 'adaptive'
    }
)

s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    config=config
)
```

## Configuration Patterns

### Environment-Based Configuration
```python
import boto3
import os
from botocore.config import Config

def create_lakefs_client():
    """Create lakeFS S3 client with environment-based configuration"""
    
    # Required environment variables
    endpoint_url = os.getenv('LAKEFS_ENDPOINT')
    access_key = os.getenv('LAKEFS_ACCESS_KEY_ID')
    secret_key = os.getenv('LAKEFS_SECRET_ACCESS_KEY')
    
    if not all([endpoint_url, access_key, secret_key]):
        raise ValueError("Missing required lakeFS environment variables")
    
    # Optional configuration
    config_params = {}
    
    # SSL configuration
    if os.getenv('LAKEFS_VERIFY_SSL', 'true').lower() == 'false':
        config_params['verify'] = False
    
    ca_cert_path = os.getenv('LAKEFS_CA_CERT_PATH')
    if ca_cert_path:
        config_params['verify'] = ca_cert_path
    
    # Proxy configuration
    http_proxy = os.getenv('HTTP_PROXY')
    https_proxy = os.getenv('HTTPS_PROXY')
    if http_proxy or https_proxy:
        proxies = {}
        if http_proxy:
            proxies['http'] = http_proxy
        if https_proxy:
            proxies['https'] = https_proxy
        
        config_params['config'] = Config(proxies=proxies)
    
    # Create client
    return boto3.client('s3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        **config_params
    )

# Usage
s3_client = create_lakefs_client()
```

### Configuration Class
```python
import boto3
from botocore.config import Config
from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class LakeFSConfig:
    """Configuration class for lakeFS Boto3 client"""
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    verify_ssl: bool = True
    ca_cert_path: Optional[str] = None
    proxies: Optional[Dict[str, str]] = None
    connect_timeout: int = 10
    read_timeout: int = 30
    max_retries: int = 3
    max_pool_connections: int = 50

    def create_client(self):
        """Create Boto3 S3 client with this configuration"""
        
        # Build config
        config_params = {
            'connect_timeout': self.connect_timeout,
            'read_timeout': self.read_timeout,
            'max_pool_connections': self.max_pool_connections,
            'retries': {
                'max_attempts': self.max_retries,
                'mode': 'adaptive'
            }
        }
        
        if self.proxies:
            config_params['proxies'] = self.proxies
        
        # Handle SSL verification
        verify = self.verify_ssl
        if self.ca_cert_path:
            verify = self.ca_cert_path
        
        return boto3.client('s3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            verify=verify,
            config=Config(**config_params)
        )

# Usage
config = LakeFSConfig(
    endpoint_url='http://localhost:8000',
    access_key_id='AKIAIOSFODNN7EXAMPLE',
    secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    verify_ssl=True,
    max_retries=5
)

s3_client = config.create_client()
```

## Session and Credential Management

### Using Boto3 Sessions
```python
import boto3

# Create session with lakeFS credentials
session = boto3.Session(
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# Create S3 client from session
s3_client = session.client('s3',
    endpoint_url='http://localhost:8000'
)

# Create S3 resource from session
s3_resource = session.resource('s3',
    endpoint_url='http://localhost:8000'
)
```

### Credential Providers
```python
import boto3
from botocore.credentials import StaticCredentialsProvider

# Custom credential provider
credentials = StaticCredentialsProvider(
    access_key='AKIAIOSFODNN7EXAMPLE',
    secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# Create client with custom credentials
s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000'
)
```

## Testing Configuration

### Configuration Validation
```python
def validate_lakefs_connection(s3_client):
    """Validate lakeFS connection and configuration"""
    try:
        # Test basic connectivity
        response = s3_client.list_buckets()
        print("✓ Connection successful")
        print(f"✓ Found {len(response.get('Buckets', []))} repositories")
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

def test_lakefs_operations(s3_client, test_repo='test-repo'):
    """Test basic lakeFS operations"""
    try:
        # Test object operations
        test_key = 'main/test-file.txt'
        test_content = b'Test content'
        
        # Upload test object
        s3_client.put_object(
            Bucket=test_repo,
            Key=test_key,
            Body=test_content
        )
        print("✓ Upload successful")
        
        # Download test object
        response = s3_client.get_object(Bucket=test_repo, Key=test_key)
        downloaded_content = response['Body'].read()
        
        if downloaded_content == test_content:
            print("✓ Download successful")
        else:
            print("✗ Download content mismatch")
        
        # Clean up
        s3_client.delete_object(Bucket=test_repo, Key=test_key)
        print("✓ Cleanup successful")
        
        return True
        
    except Exception as e:
        print(f"✗ Operation test failed: {e}")
        return False

# Usage
s3_client = create_lakefs_client()
if validate_lakefs_connection(s3_client):
    test_lakefs_operations(s3_client)
```

## Common Configuration Issues

### Troubleshooting SSL Issues
```python
import boto3
import ssl
import urllib3

# Disable SSL warnings for development
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# For SSL certificate issues
try:
    s3_client = boto3.client('s3',
        endpoint_url='https://lakefs.example.com',
        aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
        aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        verify=True
    )
    s3_client.list_buckets()
except ssl.SSLError as e:
    print(f"SSL Error: {e}")
    print("Consider using verify=False for development or provide CA certificate")
```

### Handling Checksum Errors
```python
import boto3
from botocore.config import Config

# Configuration to handle checksum issues with newer Boto3
def create_compatible_client():
    """Create client compatible with newer Boto3 versions"""
    config = Config(
        # Disable automatic checksums that may cause issues
        request_checksum_calculation='when_required',
        response_checksum_validation='when_required',
        # Alternative: disable checksums entirely
        # disable_request_compression=True
    )
    
    return boto3.client('s3',
        endpoint_url='http://localhost:8000',
        aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
        aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        config=config
    )
```

## Next Steps

- Learn about [S3 operations](s3-operations.md) with lakeFS
- Explore [S3 Router](s3-router.md) for hybrid workflows
- Review [troubleshooting guide](../reference/troubleshooting.md) for common issues