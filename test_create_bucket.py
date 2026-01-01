#!/usr/bin/env python3
"""
Test script to verify CreateBucket behavior in lakeFS S3 Gateway.

This script:
1. Creates a repository using lakeFS SDK
2. Tests CreateBucket on existing repository (should return BucketAlreadyExists)
3. Tests CreateBucket on non-existent repository (should return NotImplemented)
4. Verifies PutObject works on the existing repository

Reads configuration from ~/.lakectl.yaml
"""

import sys
import os
import yaml
import boto3
from botocore.exceptions import ClientError
import lakefs_sdk


def load_lakectl_config():
    """Load configuration from ~/.lakectl.yaml"""
    config_path = os.path.expanduser("~/.lakectl.yaml")
    
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    credentials = config.get('credentials', {})
    server = config.get('server', {})
    
    return {
        'endpoint_url': server.get('endpoint_url', 'http://localhost:8000'),
        'access_key_id': credentials.get('access_key_id'),
        'secret_access_key': credentials.get('secret_access_key'),
    }


def create_lakefs_client(config):
    """Create lakeFS SDK client"""
    configuration = lakefs_sdk.Configuration(
        host=config['endpoint_url'] + '/api/v1',
        username=config['access_key_id'],
        password=config['secret_access_key'],
    )
    client = lakefs_sdk.ApiClient(configuration)
    return client


def create_s3_client(config):
    """Create boto3 S3 client for lakeFS S3 Gateway"""
    # Extract S3 gateway endpoint (remove /api/v1 if present, use port 8000)
    endpoint = config['endpoint_url'].replace('/api/v1', '')
    
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=config['access_key_id'],
        aws_secret_access_key=config['secret_access_key'],
        region_name='us-east-1',
    )


def create_repository(client, repo_name, storage_namespace):
    """Create a repository using lakeFS SDK"""
    print(f"\n1. Creating repository '{repo_name}'...")
    
    try:
        repos_api = lakefs_sdk.RepositoriesApi(client)
        repo_creation = lakefs_sdk.RepositoryCreation(
            name=repo_name,
            storage_namespace=storage_namespace,
            default_branch="main",
        )
        
        repo = repos_api.create_repository(repo_creation)
        print(f"   ✓ Repository '{repo_name}' created successfully")
        return repo
    except lakefs_sdk.ApiException as e:
        if e.status == 409:
            print(f"   ℹ Repository '{repo_name}' already exists")
            repos_api = lakefs_sdk.RepositoriesApi(client)
            return repos_api.get_repository(repo_name)
        else:
            print(f"   ✗ Error creating repository: {e}")
            raise


def test_create_bucket_existing(s3_client, repo_name):
    """Test CreateBucket on an existing repository (should fail with BucketAlreadyExists)"""
    print(f"\n2. Testing CreateBucket on existing repository '{repo_name}'...")
    
    try:
        s3_client.create_bucket(Bucket=repo_name)
        print(f"   ✗ UNEXPECTED: CreateBucket succeeded (should have failed)")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        status_code = e.response['ResponseMetadata']['HTTPStatusCode']
        
        print(f"   Error Code: {error_code}")
        print(f"   Error Message: {error_message}")
        print(f"   HTTP Status: {status_code}")
        
        if error_code == 'BucketAlreadyExists' and status_code == 409:
            print(f"   ✓ CORRECT: Got BucketAlreadyExists (409) as expected")
            return True
        else:
            print(f"   ✗ WRONG: Expected BucketAlreadyExists (409), got {error_code} ({status_code})")
            return False


def test_create_bucket_nonexistent(s3_client, repo_name):
    """Test CreateBucket on a non-existent repository (should fail with NotImplemented)"""
    print(f"\n3. Testing CreateBucket on non-existent repository '{repo_name}'...")
    
    try:
        s3_client.create_bucket(Bucket=repo_name)
        print(f"   ✗ UNEXPECTED: CreateBucket succeeded (should have failed)")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        status_code = e.response['ResponseMetadata']['HTTPStatusCode']
        
        print(f"   Error Code: {error_code}")
        print(f"   Error Message: {error_message}")
        print(f"   HTTP Status: {status_code}")
        
        if error_code == 'NotImplemented' and status_code == 501:
            print(f"   ✓ CORRECT: Got NotImplemented (501) as expected")
            return True
        elif error_code == 'NoSuchBucket' and status_code == 404:
            print(f"   ✗ WRONG: Got NoSuchBucket (404) - this is the bug we're fixing!")
            print(f"   The fix hasn't been applied yet.")
            return False
        else:
            print(f"   ✗ UNEXPECTED: Got {error_code} ({status_code})")
            return False


def test_put_object(s3_client, repo_name):
    """Test PutObject on the existing repository (should work)"""
    print(f"\n4. Testing PutObject on existing repository '{repo_name}'...")
    
    key = "main/test-file.txt"
    content = b"Hello from lakeFS CreateBucket test!"
    
    try:
        s3_client.put_object(
            Bucket=repo_name,
            Key=key,
            Body=content
        )
        print(f"   ✓ Successfully uploaded object '{key}'")
        
        # Verify we can read it back
        response = s3_client.get_object(Bucket=repo_name, Key=key)
        retrieved_content = response['Body'].read()
        
        if retrieved_content == content:
            print(f"   ✓ Successfully retrieved object with correct content")
            return True
        else:
            print(f"   ✗ Retrieved content doesn't match uploaded content")
            return False
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"   ✗ Error: {error_code} - {e.response['Error']['Message']}")
        return False


def cleanup_repository(client, repo_name):
    """Delete the test repository"""
    print(f"\n5. Cleaning up repository '{repo_name}'...")
    
    try:
        repos_api = lakefs_sdk.RepositoriesApi(client)
        repos_api.delete_repository(repo_name)
        print(f"   ✓ Repository '{repo_name}' deleted successfully")
    except lakefs_sdk.ApiException as e:
        if e.status == 404:
            print(f"   ℹ Repository '{repo_name}' doesn't exist")
        else:
            print(f"   ✗ Error deleting repository: {e}")


def main():
    print("=" * 70)
    print("lakeFS S3 Gateway CreateBucket Test")
    print("=" * 70)
    
    # Load configuration
    config = load_lakectl_config()
    print(f"\nConfiguration loaded from ~/.lakectl.yaml")
    print(f"  Endpoint: {config['endpoint_url']}")
    print(f"  Access Key: {config['access_key_id'][:8]}...")
    
    # Create clients
    lakefs_client = create_lakefs_client(config)
    s3_client = create_s3_client(config)
    
    # Test repository names
    existing_repo = "test-create-bucket-existing"
    nonexistent_repo = "test-create-bucket-nonexistent"
    
    # Determine storage namespace based on endpoint
    # For local testing, use local:// namespace
    storage_namespace = f"local://{existing_repo}"
    
    results = {}
    
    try:
        # Test 1: Create repository
        create_repository(lakefs_client, existing_repo, storage_namespace)
        
        # Test 2: CreateBucket on existing repository
        results['existing_bucket'] = test_create_bucket_existing(s3_client, existing_repo)
        
        # Test 3: CreateBucket on non-existent repository
        results['nonexistent_bucket'] = test_create_bucket_nonexistent(s3_client, nonexistent_repo)
        
        # Test 4: PutObject on existing repository
        results['put_object'] = test_put_object(s3_client, existing_repo)
        
    finally:
        # Cleanup
        cleanup_repository(lakefs_client, existing_repo)
    
    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False
    
    print("=" * 70)
    
    if all_passed:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
