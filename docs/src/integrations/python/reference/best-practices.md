---
title: Production Best Practices
description: Comprehensive guide for deploying Python lakeFS applications in production
sdk_types: ["high-level", "generated", "lakefs-spec", "boto3"]
difficulty: "advanced"
use_cases: ["production", "deployment", "performance"]
---

# Production Best Practices

This guide covers essential practices for deploying and operating Python applications with lakeFS in production environments, including performance optimization, security considerations, monitoring, and operational best practices.

## Connection Management and Performance

### Connection Pooling

Proper connection management is crucial for production performance and resource utilization.

#### High-Level SDK Connection Pooling

```python
import lakefs
from lakefs.config import Config

# Configure connection pooling
config = Config(
    host="https://lakefs.example.com",
    access_key_id="your-access-key",
    secret_access_key="your-secret-key",
    # Connection pool settings
    pool_connections=30,
    pool_maxsize=30,
    max_retries=3,
    backoff_factor=0.3
)

# Create client with optimized settings
client = lakefs.Client(config=config)

# Reuse client across your application
class DataService:
    def __init__(self):
        self.client = client  # Reuse the same client instance
    
    def process_data(self, repo_name, branch_name):
        repo = self.client.repository(repo_name)
        branch = repo.branch(branch_name)
        # Operations use the pooled connections
        return branch.objects()
```

#### Generated SDK Connection Configuration

```python
from lakefs_sdk import Configuration, ApiClient
import urllib3

# Configure connection pooling for Generated SDK
configuration = Configuration(
    host="https://lakefs.example.com",
    access_token="your-access-token"
)

# Configure urllib3 pool manager
http = urllib3.PoolManager(
    num_pools=10,
    maxsize=30,
    retries=urllib3.Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504]
    )
)

# Create API client with custom pool manager
api_client = ApiClient(configuration)
api_client.rest_client.pool_manager = http
```

#### lakefs-spec Connection Optimization

```python
import lakefs_spec
import fsspec

# Configure lakefs-spec with connection pooling
fs = fsspec.filesystem(
    'lakefs',
    host='https://lakefs.example.com',
    access_key_id='your-access-key',
    secret_access_key='your-secret-key',
    # Connection pool settings
    client_kwargs={
        'pool_connections': 30,
        'pool_maxsize': 30,
        'max_retries': 3
    }
)

# Use the same filesystem instance across operations
def process_files(file_paths):
    for path in file_paths:
        with fs.open(path, 'rb') as f:
            # Process file using the pooled connection
            data = f.read()
            yield process_data(data)
```

### Performance Optimization Techniques

#### Batch Operations

```python
# High-Level SDK: Efficient batch operations
import lakefs
from concurrent.futures import ThreadPoolExecutor, as_completed

client = lakefs.Client()
repo = client.repository("my-repo")
branch = repo.branch("main")

# Batch upload with threading
def upload_file(file_info):
    path, data = file_info
    return branch.object(path).upload(data)

files_to_upload = [
    ("data/file1.csv", data1),
    ("data/file2.csv", data2),
    # ... more files
]

# Use thread pool for concurrent uploads
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(upload_file, file_info) 
               for file_info in files_to_upload]
    
    for future in as_completed(futures):
        try:
            result = future.result()
            print(f"Upload completed: {result}")
        except Exception as e:
            print(f"Upload failed: {e}")
```

#### Streaming for Large Files

```python
# High-Level SDK: Streaming large files
def stream_large_file(repo, branch_name, object_path, local_path):
    branch = repo.branch(branch_name)
    obj = branch.object(object_path)
    
    # Stream upload for large files
    with open(local_path, 'rb') as f:
        obj.upload(f, mode='rb')
    
    # Stream download for large files
    with obj.reader() as reader:
        with open(local_path + '.downloaded', 'wb') as f:
            for chunk in reader:
                f.write(chunk)

# lakefs-spec: Efficient streaming
import lakefs_spec

def stream_with_lakefs_spec(source_path, dest_path):
    # Direct streaming without loading into memory
    with fsspec.open(source_path, 'rb') as src:
        with fsspec.open(dest_path, 'wb') as dst:
            # Stream in chunks
            while True:
                chunk = src.read(8192)  # 8KB chunks
                if not chunk:
                    break
                dst.write(chunk)
```

#### Caching Strategies

```python
import functools
import time
from typing import Dict, Any

# Repository metadata caching
class CachedLakeFSClient:
    def __init__(self, client):
        self.client = client
        self._repo_cache = {}
        self._cache_ttl = 300  # 5 minutes
    
    @functools.lru_cache(maxsize=128)
    def get_repository_info(self, repo_name: str) -> Dict[str, Any]:
        """Cache repository metadata"""
        repo = self.client.repository(repo_name)
        return {
            'name': repo.id,
            'creation_date': repo.creation_date,
            'default_branch': repo.default_branch
        }
    
    def get_branch_with_cache(self, repo_name: str, branch_name: str):
        """Cache branch objects for reuse"""
        cache_key = f"{repo_name}:{branch_name}"
        current_time = time.time()
        
        if (cache_key in self._repo_cache and 
            current_time - self._repo_cache[cache_key]['timestamp'] < self._cache_ttl):
            return self._repo_cache[cache_key]['branch']
        
        repo = self.client.repository(repo_name)
        branch = repo.branch(branch_name)
        
        self._repo_cache[cache_key] = {
            'branch': branch,
            'timestamp': current_time
        }
        
        return branch
```

## Security Best Practices

### Credential Management

#### Environment-Based Configuration

```python
import os
import lakefs
from lakefs.config import Config

# Production credential management
def create_secure_client():
    # Never hardcode credentials
    config = Config(
        host=os.environ['LAKEFS_HOST'],
        access_key_id=os.environ['LAKEFS_ACCESS_KEY_ID'],
        secret_access_key=os.environ['LAKEFS_SECRET_ACCESS_KEY']
    )
    
    # Validate configuration
    if not all([config.host, config.access_key_id, config.secret_access_key]):
        raise ValueError("Missing required lakeFS credentials")
    
    return lakefs.Client(config=config)

# Use AWS Secrets Manager or similar
import boto3
import json

def get_lakefs_credentials_from_secrets():
    """Retrieve credentials from AWS Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    
    try:
        response = secrets_client.get_secret_value(
            SecretId='lakefs/production/credentials'
        )
        credentials = json.loads(response['SecretString'])
        
        return Config(
            host=credentials['host'],
            access_key_id=credentials['access_key_id'],
            secret_access_key=credentials['secret_access_key']
        )
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve credentials: {e}")
```

#### SSL/TLS Configuration

```python
import ssl
import lakefs
from lakefs.config import Config

# Production SSL configuration
def create_secure_ssl_client():
    config = Config(
        host="https://lakefs.example.com",
        access_key_id=os.environ['LAKEFS_ACCESS_KEY_ID'],
        secret_access_key=os.environ['LAKEFS_SECRET_ACCESS_KEY'],
        # SSL configuration
        verify_ssl=True,
        ssl_ca_cert="/path/to/ca-bundle.crt",  # Custom CA if needed
        cert_file="/path/to/client.crt",       # Client certificate
        key_file="/path/to/client.key"         # Client private key
    )
    
    return lakefs.Client(config=config)

# For Generated SDK
from lakefs_sdk import Configuration
import urllib3

def configure_ssl_for_generated_sdk():
    configuration = Configuration(
        host="https://lakefs.example.com",
        access_token="your-token"
    )
    
    # Custom SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    
    # Configure urllib3 with custom SSL
    http = urllib3.PoolManager(
        ssl_context=ssl_context,
        cert_reqs='CERT_REQUIRED',
        ca_certs='/path/to/ca-bundle.crt'
    )
    
    return configuration, http
```

### Access Control and Permissions

```python
import lakefs
from typing import List, Dict

class SecureLakeFSService:
    """Production service with access control"""
    
    def __init__(self, config):
        self.client = lakefs.Client(config)
        self.allowed_repositories = self._load_allowed_repositories()
        self.user_permissions = self._load_user_permissions()
    
    def _load_allowed_repositories(self) -> List[str]:
        """Load allowed repositories from configuration"""
        return os.environ.get('ALLOWED_REPOSITORIES', '').split(',')
    
    def _validate_repository_access(self, repo_name: str, user_id: str):
        """Validate user has access to repository"""
        if repo_name not in self.allowed_repositories:
            raise PermissionError(f"Access denied to repository: {repo_name}")
        
        user_perms = self.user_permissions.get(user_id, [])
        if repo_name not in user_perms:
            raise PermissionError(f"User {user_id} lacks access to {repo_name}")
    
    def safe_repository_access(self, repo_name: str, user_id: str):
        """Safely access repository with validation"""
        self._validate_repository_access(repo_name, user_id)
        return self.client.repository(repo_name)
```

## Deployment Best Practices

### Containerization

#### Docker Configuration

```dockerfile
# Production Dockerfile
FROM python:3.11-slim

# Create non-root user
RUN groupadd -r lakefs && useradd -r -g lakefs lakefs

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY --chown=lakefs:lakefs . /app
WORKDIR /app

# Switch to non-root user
USER lakefs

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import lakefs; client = lakefs.Client(); client.repositories.list()" || exit 1

CMD ["python", "app.py"]
```

#### Kubernetes Deployment

```yaml
# production-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lakefs-python-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: lakefs-python-app
  template:
    metadata:
      labels:
        app: lakefs-python-app
    spec:
      containers:
      - name: app
        image: your-registry/lakefs-python-app:latest
        env:
        - name: LAKEFS_HOST
          valueFrom:
            secretKeyRef:
              name: lakefs-credentials
              key: host
        - name: LAKEFS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: lakefs-credentials
              key: access_key_id
        - name: LAKEFS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: lakefs-credentials
              key: secret_access_key
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

### Configuration Management

```python
import os
from dataclasses import dataclass
from typing import Optional
import lakefs

@dataclass
class ProductionConfig:
    """Production configuration management"""
    lakefs_host: str
    lakefs_access_key_id: str
    lakefs_secret_access_key: str
    
    # Performance settings
    connection_pool_size: int = 30
    max_retries: int = 3
    timeout: int = 30
    
    # Security settings
    verify_ssl: bool = True
    ssl_ca_cert: Optional[str] = None
    
    # Operational settings
    log_level: str = "INFO"
    metrics_enabled: bool = True
    
    @classmethod
    def from_environment(cls):
        """Load configuration from environment variables"""
        return cls(
            lakefs_host=os.environ['LAKEFS_HOST'],
            lakefs_access_key_id=os.environ['LAKEFS_ACCESS_KEY_ID'],
            lakefs_secret_access_key=os.environ['LAKEFS_SECRET_ACCESS_KEY'],
            connection_pool_size=int(os.environ.get('LAKEFS_POOL_SIZE', '30')),
            max_retries=int(os.environ.get('LAKEFS_MAX_RETRIES', '3')),
            timeout=int(os.environ.get('LAKEFS_TIMEOUT', '30')),
            verify_ssl=os.environ.get('LAKEFS_VERIFY_SSL', 'true').lower() == 'true',
            ssl_ca_cert=os.environ.get('LAKEFS_SSL_CA_CERT'),
            log_level=os.environ.get('LOG_LEVEL', 'INFO'),
            metrics_enabled=os.environ.get('METRICS_ENABLED', 'true').lower() == 'true'
        )
    
    def create_client(self) -> lakefs.Client:
        """Create configured lakeFS client"""
        config = lakefs.Config(
            host=self.lakefs_host,
            access_key_id=self.lakefs_access_key_id,
            secret_access_key=self.lakefs_secret_access_key,
            pool_connections=self.connection_pool_size,
            pool_maxsize=self.connection_pool_size,
            max_retries=self.max_retries,
            verify_ssl=self.verify_ssl,
            ssl_ca_cert=self.ssl_ca_cert
        )
        
        return lakefs.Client(config=config)
```

## Monitoring and Observability

### Logging Configuration

```python
import logging
import structlog
import lakefs
from pythonjsonlogger import jsonlogger

def setup_production_logging():
    """Configure structured logging for production"""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    handler.setFormatter(formatter)
    
    # Set up lakeFS client logging
    lakefs_logger = logging.getLogger('lakefs')
    lakefs_logger.setLevel(logging.INFO)
    lakefs_logger.addHandler(handler)
    
    return structlog.get_logger()

# Usage in application
logger = setup_production_logging()

class LakeFSService:
    def __init__(self):
        self.client = lakefs.Client()
        self.logger = logger.bind(service="lakefs")
    
    def upload_data(self, repo_name, branch_name, path, data):
        self.logger.info(
            "Starting data upload",
            repo=repo_name,
            branch=branch_name,
            path=path,
            size=len(data)
        )
        
        try:
            repo = self.client.repository(repo_name)
            branch = repo.branch(branch_name)
            result = branch.object(path).upload(data)
            
            self.logger.info(
                "Data upload completed",
                repo=repo_name,
                branch=branch_name,
                path=path,
                checksum=result.checksum
            )
            
            return result
            
        except Exception as e:
            self.logger.error(
                "Data upload failed",
                repo=repo_name,
                branch=branch_name,
                path=path,
                error=str(e),
                exc_info=True
            )
            raise
```

### Metrics and Monitoring

```python
import time
import functools
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import lakefs

# Prometheus metrics
LAKEFS_OPERATIONS = Counter(
    'lakefs_operations_total',
    'Total lakeFS operations',
    ['operation', 'repository', 'status']
)

LAKEFS_OPERATION_DURATION = Histogram(
    'lakefs_operation_duration_seconds',
    'Duration of lakeFS operations',
    ['operation', 'repository']
)

LAKEFS_ACTIVE_CONNECTIONS = Gauge(
    'lakefs_active_connections',
    'Number of active lakeFS connections'
)

def monitor_lakefs_operation(operation_name):
    """Decorator to monitor lakeFS operations"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            repo_name = kwargs.get('repo_name', 'unknown')
            
            start_time = time.time()
            LAKEFS_ACTIVE_CONNECTIONS.inc()
            
            try:
                result = func(*args, **kwargs)
                LAKEFS_OPERATIONS.labels(
                    operation=operation_name,
                    repository=repo_name,
                    status='success'
                ).inc()
                return result
                
            except Exception as e:
                LAKEFS_OPERATIONS.labels(
                    operation=operation_name,
                    repository=repo_name,
                    status='error'
                ).inc()
                raise
                
            finally:
                duration = time.time() - start_time
                LAKEFS_OPERATION_DURATION.labels(
                    operation=operation_name,
                    repository=repo_name
                ).observe(duration)
                LAKEFS_ACTIVE_CONNECTIONS.dec()
        
        return wrapper
    return decorator

# Usage
class MonitoredLakeFSService:
    def __init__(self):
        self.client = lakefs.Client()
    
    @monitor_lakefs_operation('upload')
    def upload_file(self, repo_name, branch_name, path, data):
        repo = self.client.repository(repo_name)
        branch = repo.branch(branch_name)
        return branch.object(path).upload(data)
    
    @monitor_lakefs_operation('download')
    def download_file(self, repo_name, branch_name, path):
        repo = self.client.repository(repo_name)
        branch = repo.branch(branch_name)
        return branch.object(path).reader().read()

# Start metrics server
start_http_server(8000)
```

### Health Checks

```python
import lakefs
from typing import Dict, Any
import time

class HealthChecker:
    """Production health checking for lakeFS connectivity"""
    
    def __init__(self, client: lakefs.Client):
        self.client = client
        self.last_check = 0
        self.check_interval = 30  # seconds
        self.cached_status = None
    
    def check_health(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        current_time = time.time()
        
        # Use cached result if recent
        if (self.cached_status and 
            current_time - self.last_check < self.check_interval):
            return self.cached_status
        
        health_status = {
            'timestamp': current_time,
            'status': 'healthy',
            'checks': {}
        }
        
        # Check basic connectivity
        try:
            start_time = time.time()
            repos = list(self.client.repositories.list(amount=1))
            response_time = time.time() - start_time
            
            health_status['checks']['connectivity'] = {
                'status': 'pass',
                'response_time': response_time
            }
            
        except Exception as e:
            health_status['status'] = 'unhealthy'
            health_status['checks']['connectivity'] = {
                'status': 'fail',
                'error': str(e)
            }
        
        # Check authentication
        try:
            # Try to access user info or perform authenticated operation
            self.client.repositories.list(amount=1)
            health_status['checks']['authentication'] = {
                'status': 'pass'
            }
            
        except Exception as e:
            health_status['status'] = 'unhealthy'
            health_status['checks']['authentication'] = {
                'status': 'fail',
                'error': str(e)
            }
        
        self.cached_status = health_status
        self.last_check = current_time
        
        return health_status
    
    def is_healthy(self) -> bool:
        """Simple boolean health check"""
        return self.check_health()['status'] == 'healthy'

# Flask health endpoint example
from flask import Flask, jsonify

app = Flask(__name__)
health_checker = HealthChecker(lakefs.Client())

@app.route('/health')
def health():
    health_status = health_checker.check_health()
    status_code = 200 if health_status['status'] == 'healthy' else 503
    return jsonify(health_status), status_code

@app.route('/ready')
def ready():
    # Readiness check - can be more strict than health
    if health_checker.is_healthy():
        return jsonify({'status': 'ready'}), 200
    else:
        return jsonify({'status': 'not ready'}), 503
```

## Error Handling and Resilience

### Retry Strategies

```python
import time
import random
from functools import wraps
from typing import Callable, Type, Tuple
import lakefs

def exponential_backoff_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """Exponential backoff retry decorator"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        raise
                    
                    # Calculate delay with jitter
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    jitter = random.uniform(0, delay * 0.1)
                    time.sleep(delay + jitter)
                    
                    logger.warning(
                        f"Retry attempt {attempt + 1}/{max_retries} for {func.__name__}",
                        error=str(e),
                        delay=delay + jitter
                    )
            
            raise last_exception
        
        return wrapper
    return decorator

# Usage
class ResilientLakeFSService:
    def __init__(self):
        self.client = lakefs.Client()
    
    @exponential_backoff_retry(
        max_retries=3,
        exceptions=(lakefs.exceptions.ServerException, ConnectionError)
    )
    def upload_with_retry(self, repo_name, branch_name, path, data):
        repo = self.client.repository(repo_name)
        branch = repo.branch(branch_name)
        return branch.object(path).upload(data)
```

### Circuit Breaker Pattern

```python
import time
from enum import Enum
from typing import Callable, Any
import lakefs

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Circuit breaker for lakeFS operations"""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception
    ):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        return (time.time() - self.last_failure_time) >= self.timeout
    
    def _on_success(self):
        """Handle successful operation"""
        self.failure_count = 0
        self.state = CircuitState.CLOSED
    
    def _on_failure(self):
        """Handle failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN

# Usage
lakefs_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    timeout=60.0,
    expected_exception=lakefs.exceptions.ServerException
)

class ProtectedLakeFSService:
    def __init__(self):
        self.client = lakefs.Client()
    
    def safe_upload(self, repo_name, branch_name, path, data):
        """Upload with circuit breaker protection"""
        def upload_operation():
            repo = self.client.repository(repo_name)
            branch = repo.branch(branch_name)
            return branch.object(path).upload(data)
        
        return lakefs_circuit_breaker.call(upload_operation)
```

## Performance Monitoring and Optimization

### Performance Profiling

```python
import cProfile
import pstats
import io
from contextlib import contextmanager
import lakefs

@contextmanager
def profile_lakefs_operation(operation_name: str):
    """Context manager for profiling lakeFS operations"""
    profiler = cProfile.Profile()
    profiler.enable()
    
    try:
        yield
    finally:
        profiler.disable()
        
        # Analyze results
        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s)
        ps.sort_stats('cumulative')
        ps.print_stats(20)  # Top 20 functions
        
        logger.info(
            f"Performance profile for {operation_name}",
            profile_data=s.getvalue()
        )

# Usage
def analyze_performance():
    client = lakefs.Client()
    repo = client.repository("my-repo")
    branch = repo.branch("main")
    
    with profile_lakefs_operation("batch_upload"):
        # Perform operations to profile
        for i in range(100):
            branch.object(f"data/file_{i}.txt").upload(f"data_{i}")
```

### Memory Usage Optimization

```python
import gc
import psutil
import os
from typing import Iterator
import lakefs

class MemoryEfficientProcessor:
    """Memory-efficient processing of large datasets"""
    
    def __init__(self, client: lakefs.Client):
        self.client = client
        self.process = psutil.Process(os.getpid())
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024
    
    def process_large_dataset(
        self,
        repo_name: str,
        branch_name: str,
        file_paths: list,
        chunk_size: int = 1000
    ) -> Iterator[Any]:
        """Process large dataset in memory-efficient chunks"""
        
        repo = self.client.repository(repo_name)
        branch = repo.branch(branch_name)
        
        initial_memory = self.get_memory_usage()
        logger.info(f"Starting processing, initial memory: {initial_memory:.2f} MB")
        
        for i in range(0, len(file_paths), chunk_size):
            chunk = file_paths[i:i + chunk_size]
            
            # Process chunk
            for file_path in chunk:
                obj = branch.object(file_path)
                with obj.reader() as reader:
                    # Process data in streaming fashion
                    for line in reader:
                        yield self.process_line(line)
            
            # Force garbage collection after each chunk
            gc.collect()
            
            current_memory = self.get_memory_usage()
            logger.info(
                f"Processed chunk {i//chunk_size + 1}, "
                f"memory: {current_memory:.2f} MB, "
                f"delta: {current_memory - initial_memory:.2f} MB"
            )
    
    def process_line(self, line: bytes) -> Any:
        """Process individual line - implement your logic here"""
        return line.decode().strip()
```

## See Also

- [API Comparison](api-comparison.md) - Choose the right SDK for your use case
- [Troubleshooting Guide](troubleshooting.md) - Common issues and solutions
- [High-Level SDK Advanced Features](../high-level-sdk/advanced.md) - Advanced SDK capabilities
- [Generated SDK Examples](../generated-sdk/examples.md) - Direct API usage patterns
- [Security Documentation](../../security/index.md) - lakeFS security best practices
- [Deployment Guide](../../deploy/index.md) - lakeFS deployment options