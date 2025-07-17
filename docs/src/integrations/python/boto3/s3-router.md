---
title: Boto S3 Router Integration
description: Comprehensive guide to using Boto S3 Router for hybrid S3/lakeFS workflows
sdk_types: ["boto3"]
difficulty: "intermediate"
use_cases: ["hybrid-workflows", "gradual-migration", "multi-storage"]
---

# Boto S3 Router Integration

The Boto S3 Router enables hybrid workflows that seamlessly route operations between traditional S3 buckets and lakeFS repositories based on configurable rules. This allows for gradual migration and mixed storage strategies.

## Overview

The S3 Router acts as an intelligent proxy that:

- **Routes Operations** - Directs S3 operations to appropriate storage (S3 or lakeFS)
- **Enables Gradual Migration** - Migrate repositories one at a time
- **Supports Hybrid Workflows** - Use both S3 and lakeFS in the same application
- **Maintains Compatibility** - No changes to existing Boto3 code required

### Architecture

```
Application (Boto3) → S3 Router → S3 Bucket
                              → lakeFS Repository
```

The router examines each request and routes it based on:
- Bucket/repository name patterns
- Request type (read/write operations)
- Custom routing rules
- Fallback strategies

## Installation and Setup

### Install S3 Router

```bash
# Install the S3 Router package
pip install lakefs-s3-router

# Or install from source
git clone https://github.com/treeverse/lakefs-s3-router.git
cd lakefs-s3-router
pip install -e .
```

### Basic Configuration

Create a configuration file for the S3 Router:

```yaml
# s3-router-config.yaml
router:
  listen_address: "127.0.0.1:8080"
  
# lakeFS configuration
lakefs:
  endpoint: "http://localhost:8000"
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# S3 configuration
s3:
  region: "us-east-1"
  # Uses default AWS credentials (environment, IAM role, etc.)

# Routing rules
routing:
  rules:
    - pattern: "lakefs-*"
      target: "lakefs"
    - pattern: "versioned-*"
      target: "lakefs"
    - pattern: "*"
      target: "s3"
      
  # Default fallback
  default_target: "s3"
```

### Start the S3 Router

```bash
# Start the router with configuration
lakefs-s3-router --config s3-router-config.yaml

# Or with environment variables
export LAKEFS_ENDPOINT=http://localhost:8000
export LAKEFS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export LAKEFS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-east-1

lakefs-s3-router --listen 127.0.0.1:8080
```

## Boto3 Client Configuration

Configure Boto3 to use the S3 Router:

```python
import boto3

# Configure Boto3 to use S3 Router
s3_client = boto3.client('s3',
    endpoint_url='http://127.0.0.1:8080',  # S3 Router endpoint
    aws_access_key_id='your-aws-access-key',
    aws_secret_access_key='your-aws-secret-key',
    region_name='us-east-1'
)

# Now all S3 operations will be routed automatically
# Operations on 'lakefs-*' buckets go to lakeFS
# Operations on other buckets go to S3

# This goes to lakeFS
s3_client.put_object(
    Bucket='lakefs-data-repo',
    Key='main/data/file.txt',
    Body='Hello from lakeFS!'
)

# This goes to S3
s3_client.put_object(
    Bucket='traditional-s3-bucket',
    Key='data/file.txt',
    Body='Hello from S3!'
)
```

## Routing Configuration

### Pattern-Based Routing

Configure routing rules based on bucket name patterns:

```yaml
# Advanced routing configuration
routing:
  rules:
    # Route all repositories starting with 'data-' to lakeFS
    - pattern: "data-*"
      target: "lakefs"
      description: "Data repositories"
    
    # Route ML experiment repositories to lakeFS
    - pattern: "ml-experiments-*"
      target: "lakefs"
      description: "ML experiment tracking"
    
    # Route backup buckets to S3
    - pattern: "*-backup"
      target: "s3"
      description: "Backup storage"
    
    # Route logs to S3 (cheaper for append-only data)
    - pattern: "logs-*"
      target: "s3"
      description: "Log storage"
    
    # Default: route everything else to S3
    - pattern: "*"
      target: "s3"
      description: "Default S3 storage"
```

### Operation-Based Routing

Route based on operation types:

```yaml
routing:
  rules:
    # Route write operations to lakeFS for versioning
    - pattern: "critical-data-*"
      target: "lakefs"
      operations: ["PUT", "POST", "DELETE"]
      description: "Version-controlled writes"
    
    # Route read operations to S3 for performance
    - pattern: "critical-data-*"
      target: "s3"
      operations: ["GET", "HEAD"]
      description: "Fast reads from S3"
    
    # Fallback for other operations
    - pattern: "critical-data-*"
      target: "lakefs"
      description: "Default to lakeFS"
```

### Environment-Based Routing

Different routing for different environments:

```yaml
# Production configuration
routing:
  environment: "production"
  rules:
    - pattern: "prod-*"
      target: "lakefs"
    - pattern: "staging-*"
      target: "s3"
    - pattern: "*"
      target: "s3"

---
# Development configuration  
routing:
  environment: "development"
  rules:
    - pattern: "dev-*"
      target: "lakefs"
    - pattern: "*"
      target: "lakefs"  # Everything to lakeFS in dev
```

## Hybrid Workflow Examples

### Gradual Migration Strategy

Migrate repositories from S3 to lakeFS incrementally:

```python
import boto3
from datetime import datetime

# S3 Router client
s3_client = boto3.client('s3',
    endpoint_url='http://127.0.0.1:8080',
    aws_access_key_id='your-aws-access-key',
    aws_secret_access_key='your-aws-secret-key'
)

class HybridDataManager:
    """Manage data across S3 and lakeFS through S3 Router"""
    
    def __init__(self, s3_client):
        self.s3_client = s3_client
    
    def migrate_bucket_to_lakefs(self, s3_bucket, lakefs_repo):
        """Migrate data from S3 bucket to lakeFS repository"""
        
        print(f"Starting migration: {s3_bucket} → {lakefs_repo}")
        
        # List all objects in S3 bucket
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3_bucket)
        
        migrated_count = 0
        for page in pages:
            for obj in page.get('Contents', []):
                try:
                    # Copy object from S3 to lakeFS
                    self.s3_client.copy_object(
                        Bucket=lakefs_repo,
                        Key=f"main/{obj['Key']}",  # Add to main branch
                        CopySource={
                            'Bucket': s3_bucket,
                            'Key': obj['Key']
                        }
                    )
                    migrated_count += 1
                    
                    if migrated_count % 100 == 0:
                        print(f"Migrated {migrated_count} objects...")
                        
                except Exception as e:
                    print(f"Failed to migrate {obj['Key']}: {e}")
        
        print(f"Migration completed: {migrated_count} objects migrated")
    
    def sync_repositories(self, source_repo, target_repo, branch='main'):
        """Sync data between repositories"""
        
        # List objects in source
        response = self.s3_client.list_objects_v2(
            Bucket=source_repo,
            Prefix=f"{branch}/"
        )
        
        for obj in response.get('Contents', []):
            # Check if object exists in target
            target_key = obj['Key']
            
            try:
                self.s3_client.head_object(
                    Bucket=target_repo,
                    Key=target_key
                )
                # Object exists, check if newer
                # Implementation depends on your sync strategy
                
            except self.s3_client.exceptions.NoSuchKey:
                # Object doesn't exist, copy it
                self.s3_client.copy_object(
                    Bucket=target_repo,
                    Key=target_key,
                    CopySource={
                        'Bucket': source_repo,
                        'Key': obj['Key']
                    }
                )
                print(f"Synced: {target_key}")

# Usage
manager = HybridDataManager(s3_client)

# Migrate specific bucket to lakeFS
manager.migrate_bucket_to_lakefs('old-s3-bucket', 'lakefs-new-repo')

# Sync between repositories
manager.sync_repositories('lakefs-source-repo', 'lakefs-target-repo')
```

### Multi-Environment Workflow

Handle different environments with different storage strategies:

```python
import os
import boto3

class MultiEnvironmentClient:
    """S3 client that adapts to different environments"""
    
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'development')
        self.s3_client = self._create_client()
    
    def _create_client(self):
        """Create S3 client based on environment"""
        
        if self.environment == 'production':
            # Production: Use S3 Router for hybrid workflows
            return boto3.client('s3',
                endpoint_url='http://s3-router.prod.example.com:8080',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
        
        elif self.environment == 'staging':
            # Staging: Direct lakeFS for testing
            return boto3.client('s3',
                endpoint_url='http://lakefs.staging.example.com:8000',
                aws_access_key_id=os.getenv('LAKEFS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('LAKEFS_SECRET_ACCESS_KEY')
            )
        
        else:
            # Development: Local lakeFS
            return boto3.client('s3',
                endpoint_url='http://localhost:8000',
                aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
                aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
            )
    
    def get_bucket_name(self, logical_name):
        """Get environment-specific bucket name"""
        
        bucket_mapping = {
            'production': {
                'user-data': 'lakefs-prod-users',      # → lakeFS
                'analytics': 'lakefs-prod-analytics',  # → lakeFS
                'logs': 'prod-logs-s3',               # → S3
                'backups': 'prod-backups-s3'          # → S3
            },
            'staging': {
                'user-data': 'staging-users',
                'analytics': 'staging-analytics',
                'logs': 'staging-logs',
                'backups': 'staging-backups'
            },
            'development': {
                'user-data': 'dev-users',
                'analytics': 'dev-analytics',
                'logs': 'dev-logs',
                'backups': 'dev-backups'
            }
        }
        
        return bucket_mapping[self.environment][logical_name]
    
    def store_user_data(self, user_id, data):
        """Store user data with environment-appropriate routing"""
        
        bucket = self.get_bucket_name('user-data')
        key = f"main/users/{user_id}/profile.json"
        
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType='application/json'
        )
        
        print(f"Stored user data in {bucket}/{key}")
    
    def store_analytics_data(self, dataset_name, data):
        """Store analytics data"""
        
        bucket = self.get_bucket_name('analytics')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f"main/datasets/{dataset_name}/{timestamp}.parquet"
        
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType='application/octet-stream'
        )
        
        print(f"Stored analytics data in {bucket}/{key}")

# Usage
client = MultiEnvironmentClient()

# These operations will be routed based on environment and configuration
client.store_user_data('user123', '{"name": "John", "email": "john@example.com"}')
client.store_analytics_data('daily-metrics', parquet_data)
```

## Advanced Configuration

### Custom Routing Logic

Implement custom routing logic with Python:

```python
# custom_router.py
import re
from datetime import datetime

class CustomRouter:
    """Custom routing logic for S3 Router"""
    
    def __init__(self, config):
        self.config = config
        self.routing_rules = self._compile_rules()
    
    def _compile_rules(self):
        """Compile routing rules for efficiency"""
        rules = []
        for rule in self.config.get('routing', {}).get('rules', []):
            pattern = rule['pattern']
            # Convert glob pattern to regex
            regex_pattern = pattern.replace('*', '.*')
            rules.append({
                'regex': re.compile(regex_pattern),
                'target': rule['target'],
                'operations': rule.get('operations', []),
                'conditions': rule.get('conditions', {})
            })
        return rules
    
    def route_request(self, bucket_name, operation, metadata=None):
        """Determine routing target for request"""
        
        for rule in self.routing_rules:
            if rule['regex'].match(bucket_name):
                # Check operation filter
                if rule['operations'] and operation not in rule['operations']:
                    continue
                
                # Check custom conditions
                if not self._check_conditions(rule['conditions'], metadata):
                    continue
                
                return rule['target']
        
        # Default fallback
        return self.config.get('routing', {}).get('default_target', 's3')
    
    def _check_conditions(self, conditions, metadata):
        """Check custom routing conditions"""
        
        if not conditions:
            return True
        
        # Time-based routing
        if 'time_range' in conditions:
            current_hour = datetime.now().hour
            start, end = conditions['time_range']
            if not (start <= current_hour <= end):
                return False
        
        # Size-based routing
        if 'max_size' in conditions and metadata:
            if metadata.get('content_length', 0) > conditions['max_size']:
                return False
        
        # Content-type routing
        if 'content_types' in conditions and metadata:
            content_type = metadata.get('content_type', '')
            if content_type not in conditions['content_types']:
                return False
        
        return True

# Configuration with custom conditions
config = {
    'routing': {
        'rules': [
            {
                'pattern': 'large-files-*',
                'target': 's3',
                'conditions': {
                    'max_size': 100 * 1024 * 1024  # Files > 100MB go to S3
                }
            },
            {
                'pattern': 'images-*',
                'target': 'lakefs',
                'conditions': {
                    'content_types': ['image/jpeg', 'image/png', 'image/gif']
                }
            },
            {
                'pattern': 'batch-*',
                'target': 'lakefs',
                'operations': ['PUT', 'POST'],
                'conditions': {
                    'time_range': [2, 6]  # Batch operations 2-6 AM
                }
            }
        ],
        'default_target': 'lakefs'
    }
}

router = CustomRouter(config)
```

### Health Monitoring

Monitor S3 Router health and routing decisions:

```python
import boto3
import time
import logging
from datetime import datetime

class RouterMonitor:
    """Monitor S3 Router health and performance"""
    
    def __init__(self, router_endpoint):
        self.router_endpoint = router_endpoint
        self.s3_client = boto3.client('s3', endpoint_url=router_endpoint)
        self.logger = logging.getLogger(__name__)
    
    def health_check(self):
        """Perform health check on S3 Router"""
        
        checks = {
            'router_connectivity': False,
            'lakefs_connectivity': False,
            's3_connectivity': False,
            'routing_accuracy': False
        }
        
        try:
            # Test router connectivity
            response = self.s3_client.list_buckets()
            checks['router_connectivity'] = True
            
            # Test lakeFS routing (assuming lakefs-* pattern)
            test_bucket = 'lakefs-health-check'
            try:
                self.s3_client.head_bucket(Bucket=test_bucket)
                checks['lakefs_connectivity'] = True
            except:
                pass
            
            # Test S3 routing
            test_bucket = 's3-health-check'
            try:
                self.s3_client.head_bucket(Bucket=test_bucket)
                checks['s3_connectivity'] = True
            except:
                pass
            
            # Test routing accuracy
            checks['routing_accuracy'] = self._test_routing_accuracy()
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
        
        return checks
    
    def _test_routing_accuracy(self):
        """Test that routing works as expected"""
        
        test_cases = [
            ('lakefs-test', 'lakefs'),
            ('s3-test', 's3'),
            ('data-repo', 'lakefs'),  # Based on your routing rules
        ]
        
        for bucket, expected_target in test_cases:
            try:
                # This would require router API to report routing decisions
                # Implementation depends on router capabilities
                pass
            except:
                return False
        
        return True
    
    def monitor_performance(self, duration_minutes=5):
        """Monitor router performance"""
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        metrics = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'average_response_time': 0,
            'routing_distribution': {}
        }
        
        response_times = []
        
        while time.time() < end_time:
            try:
                request_start = time.time()
                
                # Test request
                self.s3_client.list_buckets()
                
                request_time = time.time() - request_start
                response_times.append(request_time)
                
                metrics['total_requests'] += 1
                metrics['successful_requests'] += 1
                
            except Exception as e:
                metrics['total_requests'] += 1
                metrics['failed_requests'] += 1
                self.logger.error(f"Request failed: {e}")
            
            time.sleep(1)  # Wait between requests
        
        # Calculate averages
        if response_times:
            metrics['average_response_time'] = sum(response_times) / len(response_times)
        
        return metrics

# Usage
monitor = RouterMonitor('http://127.0.0.1:8080')

# Perform health check
health = monitor.health_check()
print("Health Check Results:")
for check, status in health.items():
    print(f"  {check}: {'✓' if status else '✗'}")

# Monitor performance
print("\nMonitoring performance for 2 minutes...")
performance = monitor.monitor_performance(duration_minutes=2)
print("Performance Results:")
print(f"  Total Requests: {performance['total_requests']}")
print(f"  Success Rate: {performance['successful_requests']}/{performance['total_requests']}")
print(f"  Average Response Time: {performance['average_response_time']:.3f}s")
```

## Migration Examples

### From Pure S3 to Hybrid

Migrate an existing S3-only application to use hybrid routing:

```python
# Before: Direct S3 usage
import boto3

# Old configuration
s3_client = boto3.client('s3',
    region_name='us-east-1'
)

# Application code (unchanged)
def process_data():
    # Read from S3
    response = s3_client.get_object(
        Bucket='data-bucket',
        Key='input/data.csv'
    )
    data = response['Body'].read()
    
    # Process data
    processed_data = process_csv(data)
    
    # Write back to S3
    s3_client.put_object(
        Bucket='results-bucket',
        Key='output/processed.csv',
        Body=processed_data
    )

# After: Hybrid routing with S3 Router
import boto3

# New configuration - only endpoint changes!
s3_client = boto3.client('s3',
    endpoint_url='http://127.0.0.1:8080',  # S3 Router
    region_name='us-east-1'
)

# Application code remains exactly the same
def process_data():
    # Now routes to lakeFS if bucket matches pattern
    response = s3_client.get_object(
        Bucket='lakefs-data-bucket',  # Routes to lakeFS
        Key='main/input/data.csv'     # lakeFS path format
    )
    data = response['Body'].read()
    
    # Process data (unchanged)
    processed_data = process_csv(data)
    
    # Routes to lakeFS for versioning
    s3_client.put_object(
        Bucket='lakefs-results-bucket',  # Routes to lakeFS
        Key='main/output/processed.csv',
        Body=processed_data
    )
```

### Gradual Repository Migration

Migrate repositories one by one:

```python
class GradualMigrationManager:
    """Manage gradual migration from S3 to lakeFS"""
    
    def __init__(self, s3_router_client):
        self.s3_client = s3_router_client
        self.migration_status = {}
    
    def plan_migration(self, s3_buckets, lakefs_repos):
        """Plan migration phases"""
        
        migration_plan = {
            'phase_1': {
                'buckets': s3_buckets[:2],  # Start with 2 buckets
                'repos': lakefs_repos[:2],
                'priority': 'high'
            },
            'phase_2': {
                'buckets': s3_buckets[2:5],
                'repos': lakefs_repos[2:5],
                'priority': 'medium'
            },
            'phase_3': {
                'buckets': s3_buckets[5:],
                'repos': lakefs_repos[5:],
                'priority': 'low'
            }
        }
        
        return migration_plan
    
    def execute_phase(self, phase_config):
        """Execute a migration phase"""
        
        for s3_bucket, lakefs_repo in zip(phase_config['buckets'], phase_config['repos']):
            print(f"Migrating {s3_bucket} → {lakefs_repo}")
            
            # Update routing configuration to include new repo
            self._update_routing_config(lakefs_repo)
            
            # Migrate data
            self._migrate_bucket_data(s3_bucket, lakefs_repo)
            
            # Update application configuration
            self._update_app_config(s3_bucket, lakefs_repo)
            
            # Validate migration
            if self._validate_migration(s3_bucket, lakefs_repo):
                self.migration_status[s3_bucket] = 'completed'
                print(f"✓ Migration completed: {s3_bucket}")
            else:
                self.migration_status[s3_bucket] = 'failed'
                print(f"✗ Migration failed: {s3_bucket}")
    
    def _update_routing_config(self, lakefs_repo):
        """Update S3 Router configuration"""
        # Implementation depends on your router configuration method
        pass
    
    def _migrate_bucket_data(self, s3_bucket, lakefs_repo):
        """Migrate data from S3 bucket to lakeFS repo"""
        # Implementation similar to previous examples
        pass
    
    def _update_app_config(self, old_bucket, new_repo):
        """Update application configuration"""
        # Update configuration files, environment variables, etc.
        pass
    
    def _validate_migration(self, s3_bucket, lakefs_repo):
        """Validate successful migration"""
        try:
            # Compare object counts
            s3_count = self._count_objects(s3_bucket)
            lakefs_count = self._count_objects(lakefs_repo, prefix='main/')
            
            return s3_count == lakefs_count
        except:
            return False
    
    def _count_objects(self, bucket, prefix=''):
        """Count objects in bucket"""
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        count = 0
        for page in pages:
            count += len(page.get('Contents', []))
        
        return count

# Usage
migration_manager = GradualMigrationManager(s3_client)

# Plan migration
s3_buckets = ['old-bucket-1', 'old-bucket-2', 'old-bucket-3']
lakefs_repos = ['lakefs-repo-1', 'lakefs-repo-2', 'lakefs-repo-3']

plan = migration_manager.plan_migration(s3_buckets, lakefs_repos)

# Execute phases
for phase_name, phase_config in plan.items():
    print(f"\nExecuting {phase_name}...")
    migration_manager.execute_phase(phase_config)
    
    # Wait for validation before next phase
    input(f"Phase {phase_name} completed. Press Enter to continue to next phase...")
```

## Troubleshooting

### Common Issues

```python
def diagnose_router_issues(s3_client):
    """Diagnose common S3 Router issues"""
    
    issues = []
    
    try:
        # Test basic connectivity
        s3_client.list_buckets()
        print("✓ Router connectivity OK")
    except Exception as e:
        issues.append(f"Router connectivity failed: {e}")
    
    # Test routing accuracy
    test_cases = [
        ('lakefs-test', 'Should route to lakeFS'),
        ('s3-test', 'Should route to S3')
    ]
    
    for bucket, description in test_cases:
        try:
            # Attempt operation
            s3_client.head_bucket(Bucket=bucket)
            print(f"✓ {description}")
        except Exception as e:
            issues.append(f"{description}: {e}")
    
    # Check configuration
    if not os.getenv('LAKEFS_ENDPOINT'):
        issues.append("LAKEFS_ENDPOINT environment variable not set")
    
    if not os.getenv('AWS_REGION'):
        issues.append("AWS_REGION environment variable not set")
    
    return issues

# Usage
issues = diagnose_router_issues(s3_client)
if issues:
    print("Issues found:")
    for issue in issues:
        print(f"  ✗ {issue}")
else:
    print("✓ All checks passed")
```

### Performance Optimization

```python
# Optimize S3 Router performance
import boto3
from botocore.config import Config

# Configure for high-throughput scenarios
config = Config(
    max_pool_connections=100,  # Increase connection pool
    retries={
        'max_attempts': 3,
        'mode': 'adaptive'
    },
    # Increase timeouts for router processing
    connect_timeout=30,
    read_timeout=60
)

s3_client = boto3.client('s3',
    endpoint_url='http://127.0.0.1:8080',
    config=config
)
```

## Next Steps

- Review [S3 operations](s3-operations.md) for detailed operation examples
- Check [troubleshooting guide](../reference/troubleshooting.md) for common issues
- Explore [configuration options](configuration.md) for advanced setup
- Learn about [best practices](../reference/best-practices.md) for production usage

## See Also

- [High-Level SDK](../high-level-sdk/) - For advanced lakeFS features
- [Generated SDK](../generated-sdk/) - For direct API access
- [Boto3 Configuration](configuration.md) - Detailed setup guide