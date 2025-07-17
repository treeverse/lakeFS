---
title: ETL Pipeline Tutorial
description: Build production-ready ETL pipelines with lakeFS transactions and error handling
sdk_types: ["high-level", "generated"]
difficulty: "advanced"
use_cases: ["etl", "data-engineering", "production", "pipelines"]
topics: ["etl", "transactions", "error-handling", "batch-processing", "data-validation"]
audience: ["data-engineers", "backend-developers", "devops"]
last_updated: "2024-01-15"
---

# ETL Pipeline Tutorial

Learn how to build robust, production-ready ETL (Extract, Transform, Load) pipelines using lakeFS for data versioning, transactions for atomicity, and comprehensive error handling. This tutorial demonstrates enterprise-grade patterns for data processing with proper validation, monitoring, and recovery mechanisms.

## What You'll Build

By the end of this tutorial, you'll have:

- **Transactional ETL Pipeline** - Atomic data processing with rollback capabilities
- **Error Handling System** - Comprehensive error detection and recovery
- **Data Validation Framework** - Quality checks and validation rules
- **Batch Processing Engine** - Scalable data processing patterns
- **Monitoring and Alerting** - Pipeline health monitoring and notifications
- **Production Deployment** - CI/CD ready pipeline with proper logging

## Prerequisites

### Knowledge Requirements
- Intermediate Python programming
- Understanding of ETL concepts
- Basic database and SQL knowledge
- Familiarity with data validation concepts

### Environment Setup
- Python 3.8+ installed
- lakeFS server running (local or cloud)
- Database access (PostgreSQL/MySQL for examples)
- Required Python packages (we'll install these)

### lakeFS Setup
```bash
# Start lakeFS locally (if not already running)
docker run --name lakefs --rm -p 8000:8000 treeverse/lakefs:latest run --local-settings
```

## Step 1: Environment Setup and Dependencies

### Install Required Packages

```bash
# Install lakeFS and data processing libraries
pip install lakefs pandas sqlalchemy psycopg2-binary
pip install pydantic jsonschema great-expectations
pip install schedule prometheus-client structlog
pip install retry tenacity
```

### Project Structure Setup

```bash
# Create ETL project structure
mkdir lakefs-etl-pipeline
cd lakefs-etl-pipeline

# Create directory structure
mkdir -p {config,src/{extractors,transformers,loaders,validators},tests,logs,monitoring}

# Create configuration files
touch config/{database.yaml,pipeline.yaml,validation.yaml}
touch src/{__init__.py,pipeline.py,exceptions.py}
```

### Configuration Management

```python
# config/settings.py
import os
from dataclasses import dataclass
from typing import Dict, List, Optional
import yaml

@dataclass
class LakeFSConfig:
    host: str
    username: str
    password: str
    repository: str

@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    
@dataclass
class PipelineConfig:
    batch_size: int
    max_retries: int
    timeout_seconds: int
    validation_enabled: bool
    monitoring_enabled: bool

class ConfigManager:
    """Centralized configuration management"""
    
    def __init__(self, config_path: str = "config"):
        self.config_path = config_path
        self._load_configs()
    
    def _load_configs(self):
        """Load all configuration files"""
        # lakeFS configuration
        self.lakefs = LakeFSConfig(
            host=os.getenv('LAKEFS_HOST', 'http://localhost:8000'),
            username=os.getenv('LAKEFS_ACCESS_KEY', 'AKIAIOSFODNN7EXAMPLE'),
            password=os.getenv('LAKEFS_SECRET_KEY', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'),
            repository=os.getenv('LAKEFS_REPOSITORY', 'etl-pipeline')
        )
        
        # Database configuration
        self.database = DatabaseConfig(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'etl_source'),
            username=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password')
        )
        
        # Pipeline configuration
        self.pipeline = PipelineConfig(
            batch_size=int(os.getenv('BATCH_SIZE', '1000')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            timeout_seconds=int(os.getenv('TIMEOUT_SECONDS', '300')),
            validation_enabled=os.getenv('VALIDATION_ENABLED', 'true').lower() == 'true',
            monitoring_enabled=os.getenv('MONITORING_ENABLED', 'true').lower() == 'true'
        )

# Global configuration instance
config = ConfigManager()
```

## Step 2: Repository Setup and Data Models

### Create ETL Repository

```python
# src/repository_setup.py
import lakefs
from lakefs.exceptions import RepositoryExistsError
from config.settings import config
import logging

logger = logging.getLogger(__name__)

def setup_etl_repository():
    """Initialize lakeFS repository for ETL pipeline"""
    
    client = lakefs.Client(
        host=config.lakefs.host,
        username=config.lakefs.username,
        password=config.lakefs.password
    )
    
    # Create repository
    try:
        repo = client.repositories.create(
            name=config.lakefs.repository,
            storage_namespace='s3://my-bucket/etl-pipeline/',
            default_branch='main'
        )
        logger.info(f"Created repository: {repo.id}")
    except RepositoryExistsError:
        repo = client.repositories.get(config.lakefs.repository)
        logger.info(f"Using existing repository: {repo.id}")
    
    # Create standard branches
    branches_to_create = [
        ('staging', 'main'),
        ('development', 'main'),
        ('production', 'main')
    ]
    
    for branch_name, source in branches_to_create:
        try:
            branch = repo.branches.create(branch_name, source_reference=source)
            logger.info(f"Created branch: {branch_name}")
        except Exception as e:
            logger.info(f"Branch {branch_name} already exists or error: {e}")
    
    return repo

def create_directory_structure(repo):
    """Create standard directory structure in lakeFS"""
    
    main_branch = repo.branches.get('main')
    
    # Create directory structure with placeholder files
    directories = [
        'raw_data/',
        'processed_data/',
        'validated_data/',
        'failed_data/',
        'logs/',
        'schemas/',
        'checkpoints/'
    ]
    
    for directory in directories:
        try:
            main_branch.objects.upload(
                path=f"{directory}.gitkeep",
                data=b"# Directory placeholder",
                content_type='text/plain'
            )
        except Exception as e:
            logger.warning(f"Could not create {directory}: {e}")
    
    # Commit directory structure
    commit = main_branch.commits.create(
        message="Initialize ETL pipeline directory structure",
        metadata={'setup': 'directory_structure', 'pipeline': 'etl'}
    )
    
    logger.info(f"Directory structure created: {commit.id}")
    return commit

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    repo = setup_etl_repository()
    create_directory_structure(repo)
```

### Data Models and Schemas

```python
# src/models.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
import json

class PipelineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"

class ValidationStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"

@dataclass
class PipelineRun:
    """Represents a single ETL pipeline execution"""
    run_id: str
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.PENDING
    source_commit: Optional[str] = None
    target_commit: Optional[str] = None
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'run_id': self.run_id,
            'pipeline_name': self.pipeline_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status.value,
            'source_commit': self.source_commit,
            'target_commit': self.target_commit,
            'records_processed': self.records_processed,
            'records_failed': self.records_failed,
            'error_message': self.error_message,
            'metadata': self.metadata
        }

@dataclass
class ValidationResult:
    """Represents validation results for a dataset"""
    dataset_name: str
    validation_time: datetime
    status: ValidationStatus
    total_records: int
    passed_records: int
    failed_records: int
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'dataset_name': self.dataset_name,
            'validation_time': self.validation_time.isoformat(),
            'status': self.status.value,
            'total_records': self.total_records,
            'passed_records': self.passed_records,
            'failed_records': self.failed_records,
            'warnings': self.warnings,
            'errors': self.errors,
            'details': self.details
        }

@dataclass
class DataQualityMetrics:
    """Data quality metrics for monitoring"""
    dataset_name: str
    timestamp: datetime
    completeness_score: float  # 0-1
    accuracy_score: float      # 0-1
    consistency_score: float   # 0-1
    timeliness_score: float    # 0-1
    overall_score: float       # 0-1
    record_count: int
    null_percentage: float
    duplicate_percentage: float
    schema_violations: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'dataset_name': self.dataset_name,
            'timestamp': self.timestamp.isoformat(),
            'completeness_score': self.completeness_score,
            'accuracy_score': self.accuracy_score,
            'consistency_score': self.consistency_score,
            'timeliness_score': self.timeliness_score,
            'overall_score': self.overall_score,
            'record_count': self.record_count,
            'null_percentage': self.null_percentage,
            'duplicate_percentage': self.duplicate_percentage,
            'schema_violations': self.schema_violations
        }
```

## Step 3: Data Extraction Components

### Database Extractor

```python
# src/extractors/database_extractor.py
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from typing import Dict, List, Optional, Iterator
import logging
from datetime import datetime, timedelta
from config.settings import config
from src.models import PipelineRun

logger = logging.getLogger(__name__)

class DatabaseExtractor:
    """Extract data from relational databases with incremental loading support"""
    
    def __init__(self):
        self.engine = self._create_engine()
        
    def _create_engine(self) -> sa.Engine:
        """Create database engine with connection pooling"""
        db_config = config.database
        connection_string = (
            f"postgresql://{db_config.username}:{db_config.password}"
            f"@{db_config.host}:{db_config.port}/{db_config.database}"
        )
        
        return create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )
    
    def extract_full_table(self, table_name: str, chunk_size: int = None) -> Iterator[pd.DataFrame]:
        """Extract complete table data in chunks"""
        
        chunk_size = chunk_size or config.pipeline.batch_size
        
        try:
            # Get total record count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            with self.engine.connect() as conn:
                total_records = conn.execute(text(count_query)).scalar()
            
            logger.info(f"Extracting {total_records} records from {table_name}")
            
            # Extract data in chunks
            offset = 0
            while offset < total_records:
                query = f"""
                SELECT * FROM {table_name}
                ORDER BY id
                LIMIT {chunk_size} OFFSET {offset}
                """
                
                chunk_df = pd.read_sql(query, self.engine)
                
                if chunk_df.empty:
                    break
                    
                logger.debug(f"Extracted chunk: {len(chunk_df)} records (offset: {offset})")
                yield chunk_df
                
                offset += chunk_size
                
        except Exception as e:
            logger.error(f"Error extracting from {table_name}: {e}")
            raise
    
    def extract_incremental(
        self, 
        table_name: str, 
        timestamp_column: str,
        last_extracted: Optional[datetime] = None,
        chunk_size: int = None
    ) -> Iterator[pd.DataFrame]:
        """Extract data incrementally based on timestamp"""
        
        chunk_size = chunk_size or config.pipeline.batch_size
        
        # Default to 24 hours ago if no last extraction time
        if last_extracted is None:
            last_extracted = datetime.now() - timedelta(hours=24)
        
        try:
            # Build incremental query
            base_query = f"""
            SELECT * FROM {table_name}
            WHERE {timestamp_column} > %(last_extracted)s
            ORDER BY {timestamp_column}
            """
            
            # Get total incremental records
            count_query = f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE {timestamp_column} > %(last_extracted)s
            """
            
            with self.engine.connect() as conn:
                total_records = conn.execute(
                    text(count_query), 
                    {'last_extracted': last_extracted}
                ).scalar()
            
            logger.info(f"Extracting {total_records} incremental records from {table_name}")
            
            if total_records == 0:
                logger.info("No new records to extract")
                return
            
            # Extract in chunks
            offset = 0
            while offset < total_records:
                query = f"{base_query} LIMIT {chunk_size} OFFSET {offset}"
                
                chunk_df = pd.read_sql(
                    query, 
                    self.engine,
                    params={'last_extracted': last_extracted}
                )
                
                if chunk_df.empty:
                    break
                
                logger.debug(f"Extracted incremental chunk: {len(chunk_df)} records")
                yield chunk_df
                
                offset += chunk_size
                
        except Exception as e:
            logger.error(f"Error in incremental extraction from {table_name}: {e}")
            raise
    
    def extract_custom_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        """Execute custom SQL query and return results"""
        
        try:
            logger.info(f"Executing custom query: {query[:100]}...")
            
            df = pd.read_sql(query, self.engine, params=params or {})
            
            logger.info(f"Custom query returned {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get table schema information"""
        
        try:
            query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %(table_name)s
            ORDER BY ordinal_position
            """
            
            schema_df = pd.read_sql(query, self.engine, params={'table_name': table_name})
            
            return {
                row['column_name']: {
                    'data_type': row['data_type'],
                    'is_nullable': row['is_nullable']
                }
                for _, row in schema_df.iterrows()
            }
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            raise
    
    def close(self):
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")

# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    extractor = DatabaseExtractor()
    
    # Test schema extraction
    try:
        schema = extractor.get_table_schema('users')
        print("Table schema:", schema)
    except Exception as e:
        print(f"Schema extraction failed: {e}")
    
    # Test data extraction
    try:
        for chunk in extractor.extract_full_table('users', chunk_size=100):
            print(f"Extracted chunk with {len(chunk)} records")
            break  # Just test first chunk
    except Exception as e:
        print(f"Data extraction failed: {e}")
    
    extractor.close()
```## S
tep 4: Data Transformation Engine

### Core Transformation Framework

```python
# src/transformers/base_transformer.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime
from src.models import ValidationResult, ValidationStatus

logger = logging.getLogger(__name__)

class BaseTransformer(ABC):
    """Abstract base class for all data transformers"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformation to DataFrame"""
        pass
    
    def validate_input(self, df: pd.DataFrame) -> bool:
        """Validate input data before transformation"""
        if df.empty:
            self.logger.warning("Input DataFrame is empty")
            return False
        return True
    
    def validate_output(self, df: pd.DataFrame) -> bool:
        """Validate output data after transformation"""
        if df.empty:
            self.logger.warning("Output DataFrame is empty")
            return False
        return True
    
    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute transformation with validation"""
        self.logger.info(f"Starting transformation: {self.name}")
        
        if not self.validate_input(df):
            raise ValueError(f"Input validation failed for {self.name}")
        
        input_count = len(df)
        result_df = self.transform(df)
        output_count = len(result_df)
        
        if not self.validate_output(result_df):
            raise ValueError(f"Output validation failed for {self.name}")
        
        self.logger.info(
            f"Transformation {self.name} complete: {input_count} -> {output_count} records"
        )
        
        return result_df

class DataCleaningTransformer(BaseTransformer):
    """Clean and standardize data"""
    
    def __init__(self, cleaning_rules: Dict[str, Any]):
        super().__init__("DataCleaning")
        self.cleaning_rules = cleaning_rules
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data cleaning rules"""
        result_df = df.copy()
        
        # Remove duplicates
        if self.cleaning_rules.get('remove_duplicates', False):
            initial_count = len(result_df)
            result_df = result_df.drop_duplicates()
            removed_count = initial_count - len(result_df)
            if removed_count > 0:
                self.logger.info(f"Removed {removed_count} duplicate records")
        
        # Handle missing values
        null_handling = self.cleaning_rules.get('null_handling', {})
        for column, strategy in null_handling.items():
            if column in result_df.columns:
                if strategy == 'drop':
                    result_df = result_df.dropna(subset=[column])
                elif strategy == 'fill_zero':
                    result_df[column] = result_df[column].fillna(0)
                elif strategy == 'fill_mean':
                    result_df[column] = result_df[column].fillna(result_df[column].mean())
                elif isinstance(strategy, (str, int, float)):
                    result_df[column] = result_df[column].fillna(strategy)
        
        # Data type conversions
        type_conversions = self.cleaning_rules.get('type_conversions', {})
        for column, target_type in type_conversions.items():
            if column in result_df.columns:
                try:
                    if target_type == 'datetime':
                        result_df[column] = pd.to_datetime(result_df[column])
                    elif target_type == 'numeric':
                        result_df[column] = pd.to_numeric(result_df[column], errors='coerce')
                    else:
                        result_df[column] = result_df[column].astype(target_type)
                except Exception as e:
                    self.logger.warning(f"Type conversion failed for {column}: {e}")
        
        # String cleaning
        string_cleaning = self.cleaning_rules.get('string_cleaning', {})
        for column, operations in string_cleaning.items():
            if column in result_df.columns and result_df[column].dtype == 'object':
                if 'strip' in operations:
                    result_df[column] = result_df[column].str.strip()
                if 'lower' in operations:
                    result_df[column] = result_df[column].str.lower()
                if 'upper' in operations:
                    result_df[column] = result_df[column].str.upper()
                if 'remove_special_chars' in operations:
                    result_df[column] = result_df[column].str.replace(r'[^\w\s]', '', regex=True)
        
        return result_df

class BusinessLogicTransformer(BaseTransformer):
    """Apply business logic transformations"""
    
    def __init__(self, business_rules: Dict[str, Any]):
        super().__init__("BusinessLogic")
        self.business_rules = business_rules
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business logic rules"""
        result_df = df.copy()
        
        # Calculate derived columns
        derived_columns = self.business_rules.get('derived_columns', {})
        for new_column, formula in derived_columns.items():
            try:
                # Simple formula evaluation (extend as needed)
                if isinstance(formula, str):
                    result_df[new_column] = result_df.eval(formula)
                elif callable(formula):
                    result_df[new_column] = result_df.apply(formula, axis=1)
            except Exception as e:
                self.logger.error(f"Error calculating {new_column}: {e}")
                result_df[new_column] = None
        
        # Apply filters
        filters = self.business_rules.get('filters', [])
        for filter_condition in filters:
            try:
                initial_count = len(result_df)
                result_df = result_df.query(filter_condition)
                filtered_count = initial_count - len(result_df)
                if filtered_count > 0:
                    self.logger.info(f"Filtered out {filtered_count} records with: {filter_condition}")
            except Exception as e:
                self.logger.error(f"Error applying filter '{filter_condition}': {e}")
        
        # Categorization
        categorizations = self.business_rules.get('categorizations', {})
        for column, categories in categorizations.items():
            if column in result_df.columns:
                def categorize_value(value):
                    for category, condition in categories.items():
                        if callable(condition) and condition(value):
                            return category
                        elif isinstance(condition, (list, tuple)) and value in condition:
                            return category
                        elif isinstance(condition, dict):
                            if condition.get('min', float('-inf')) <= value <= condition.get('max', float('inf')):
                                return category
                    return 'Other'
                
                result_df[f"{column}_category"] = result_df[column].apply(categorize_value)
        
        return result_df

class AggregationTransformer(BaseTransformer):
    """Perform data aggregations"""
    
    def __init__(self, aggregation_rules: Dict[str, Any]):
        super().__init__("Aggregation")
        self.aggregation_rules = aggregation_rules
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply aggregation rules"""
        
        group_by = self.aggregation_rules.get('group_by', [])
        aggregations = self.aggregation_rules.get('aggregations', {})
        
        if not group_by or not aggregations:
            self.logger.warning("No grouping or aggregation rules specified")
            return df
        
        try:
            # Perform groupby aggregation
            result_df = df.groupby(group_by).agg(aggregations).reset_index()
            
            # Flatten column names if multi-level
            if isinstance(result_df.columns, pd.MultiIndex):
                result_df.columns = ['_'.join(col).strip() if col[1] else col[0] 
                                   for col in result_df.columns.values]
            
            self.logger.info(f"Aggregated {len(df)} records into {len(result_df)} groups")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Aggregation failed: {e}")
            raise

class TransformationPipeline:
    """Chain multiple transformers together"""
    
    def __init__(self, transformers: List[BaseTransformer]):
        self.transformers = transformers
        self.logger = logging.getLogger(__name__)
    
    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute all transformers in sequence"""
        
        result_df = df.copy()
        
        for transformer in self.transformers:
            try:
                result_df = transformer.execute(result_df)
            except Exception as e:
                self.logger.error(f"Transformation pipeline failed at {transformer.name}: {e}")
                raise
        
        self.logger.info(f"Transformation pipeline complete: {len(df)} -> {len(result_df)} records")
        return result_df

# Example transformation configurations
SAMPLE_CLEANING_RULES = {
    'remove_duplicates': True,
    'null_handling': {
        'email': 'drop',
        'age': 'fill_zero',
        'income': 'fill_mean',
        'status': 'active'
    },
    'type_conversions': {
        'created_at': 'datetime',
        'age': 'int',
        'income': 'float'
    },
    'string_cleaning': {
        'email': ['strip', 'lower'],
        'name': ['strip'],
        'status': ['strip', 'lower']
    }
}

SAMPLE_BUSINESS_RULES = {
    'derived_columns': {
        'age_group': lambda row: 'Young' if row['age'] < 30 else 'Middle' if row['age'] < 60 else 'Senior',
        'income_bracket': 'income // 10000 * 10000'  # Round to nearest 10k
    },
    'filters': [
        'age >= 18',
        'income > 0',
        'status == "active"'
    ],
    'categorizations': {
        'income': {
            'Low': {'min': 0, 'max': 30000},
            'Medium': {'min': 30001, 'max': 80000},
            'High': {'min': 80001, 'max': float('inf')}
        }
    }
}

SAMPLE_AGGREGATION_RULES = {
    'group_by': ['age_group', 'income_category'],
    'aggregations': {
        'income': ['mean', 'median', 'count'],
        'age': ['mean', 'min', 'max']
    }
}

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data
    sample_data = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'User {i}' for i in range(1, 101)],
        'email': [f'user{i}@example.com' if i % 10 != 0 else None for i in range(1, 101)],
        'age': [20 + (i % 50) for i in range(1, 101)],
        'income': [30000 + (i * 1000) for i in range(1, 101)],
        'status': ['active' if i % 5 != 0 else 'inactive' for i in range(1, 101)]
    })
    
    # Create transformation pipeline
    transformers = [
        DataCleaningTransformer(SAMPLE_CLEANING_RULES),
        BusinessLogicTransformer(SAMPLE_BUSINESS_RULES),
        AggregationTransformer(SAMPLE_AGGREGATION_RULES)
    ]
    
    pipeline = TransformationPipeline(transformers)
    
    # Execute pipeline
    try:
        result = pipeline.execute(sample_data)
        print("Transformation successful!")
        print(result.head())
    except Exception as e:
        print(f"Transformation failed: {e}")
```

## Step 5: Data Validation Framework

### Comprehensive Data Validation

```python
# src/validators/data_validator.py
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Callable
import logging
from datetime import datetime
import jsonschema
from src.models import ValidationResult, ValidationStatus

logger = logging.getLogger(__name__)

class ValidationRule:
    """Individual validation rule"""
    
    def __init__(self, name: str, description: str, validator_func: Callable, severity: str = 'error'):
        self.name = name
        self.description = description
        self.validator_func = validator_func
        self.severity = severity  # 'error', 'warning', 'info'
    
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Execute validation rule"""
        try:
            result = self.validator_func(df)
            return {
                'rule_name': self.name,
                'description': self.description,
                'severity': self.severity,
                'passed': result.get('passed', False),
                'message': result.get('message', ''),
                'details': result.get('details', {}),
                'affected_records': result.get('affected_records', 0)
            }
        except Exception as e:
            return {
                'rule_name': self.name,
                'description': self.description,
                'severity': 'error',
                'passed': False,
                'message': f"Validation rule execution failed: {str(e)}",
                'details': {},
                'affected_records': 0
            }

class DataValidator:
    """Comprehensive data validation framework"""
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.logger = logging.getLogger(__name__)
    
    def add_rule(self, rule: ValidationRule):
        """Add validation rule"""
        self.rules.append(rule)
        self.logger.debug(f"Added validation rule: {rule.name}")
    
    def add_schema_validation(self, schema: Dict[str, Any]):
        """Add JSON schema validation"""
        def schema_validator(df: pd.DataFrame) -> Dict[str, Any]:
            violations = []
            
            for column, column_schema in schema.get('properties', {}).items():
                if column not in df.columns:
                    if column in schema.get('required', []):
                        violations.append(f"Required column '{column}' is missing")
                    continue
                
                # Type validation
                expected_type = column_schema.get('type')
                if expected_type:
                    if expected_type == 'string' and df[column].dtype != 'object':
                        violations.append(f"Column '{column}' should be string type")
                    elif expected_type == 'number' and not pd.api.types.is_numeric_dtype(df[column]):
                        violations.append(f"Column '{column}' should be numeric type")
                    elif expected_type == 'integer' and df[column].dtype not in ['int64', 'int32']:
                        violations.append(f"Column '{column}' should be integer type")
                
                # Range validation
                if 'minimum' in column_schema:
                    min_violations = (df[column] < column_schema['minimum']).sum()
                    if min_violations > 0:
                        violations.append(f"Column '{column}' has {min_violations} values below minimum {column_schema['minimum']}")
                
                if 'maximum' in column_schema:
                    max_violations = (df[column] > column_schema['maximum']).sum()
                    if max_violations > 0:
                        violations.append(f"Column '{column}' has {max_violations} values above maximum {column_schema['maximum']}")
            
            return {
                'passed': len(violations) == 0,
                'message': '; '.join(violations) if violations else 'Schema validation passed',
                'details': {'violations': violations},
                'affected_records': len(violations)
            }
        
        rule = ValidationRule(
            name="schema_validation",
            description="Validate data against JSON schema",
            validator_func=schema_validator,
            severity='error'
        )
        self.add_rule(rule)
    
    def add_completeness_check(self, required_columns: List[str], threshold: float = 0.95):
        """Add data completeness validation"""
        def completeness_validator(df: pd.DataFrame) -> Dict[str, Any]:
            issues = []
            
            for column in required_columns:
                if column not in df.columns:
                    issues.append(f"Required column '{column}' is missing")
                    continue
                
                completeness = 1 - (df[column].isnull().sum() / len(df))
                if completeness < threshold:
                    issues.append(f"Column '{column}' completeness {completeness:.2%} below threshold {threshold:.2%}")
            
            return {
                'passed': len(issues) == 0,
                'message': '; '.join(issues) if issues else f'Completeness check passed for {len(required_columns)} columns',
                'details': {'threshold': threshold, 'issues': issues},
                'affected_records': len(issues)
            }
        
        rule = ValidationRule(
            name="completeness_check",
            description=f"Check data completeness for required columns (threshold: {threshold:.2%})",
            validator_func=completeness_validator,
            severity='error'
        )
        self.add_rule(rule)
    
    def add_uniqueness_check(self, unique_columns: List[str]):
        """Add uniqueness validation"""
        def uniqueness_validator(df: pd.DataFrame) -> Dict[str, Any]:
            issues = []
            
            for column in unique_columns:
                if column not in df.columns:
                    issues.append(f"Column '{column}' not found for uniqueness check")
                    continue
                
                duplicates = df[column].duplicated().sum()
                if duplicates > 0:
                    issues.append(f"Column '{column}' has {duplicates} duplicate values")
            
            return {
                'passed': len(issues) == 0,
                'message': '; '.join(issues) if issues else f'Uniqueness check passed for {len(unique_columns)} columns',
                'details': {'issues': issues},
                'affected_records': sum([int(issue.split()[3]) for issue in issues if 'duplicate' in issue])
            }
        
        rule = ValidationRule(
            name="uniqueness_check",
            description="Check for duplicate values in unique columns",
            validator_func=uniqueness_validator,
            severity='error'
        )
        self.add_rule(rule)
    
    def add_referential_integrity_check(self, foreign_keys: Dict[str, Dict[str, Any]]):
        """Add referential integrity validation"""
        def referential_validator(df: pd.DataFrame) -> Dict[str, Any]:
            issues = []
            
            for fk_column, reference_info in foreign_keys.items():
                if fk_column not in df.columns:
                    issues.append(f"Foreign key column '{fk_column}' not found")
                    continue
                
                # For this example, we'll check against a list of valid values
                # In practice, this would query the reference table
                valid_values = reference_info.get('valid_values', [])
                if valid_values:
                    invalid_count = (~df[fk_column].isin(valid_values)).sum()
                    if invalid_count > 0:
                        issues.append(f"Column '{fk_column}' has {invalid_count} invalid references")
            
            return {
                'passed': len(issues) == 0,
                'message': '; '.join(issues) if issues else 'Referential integrity check passed',
                'details': {'issues': issues},
                'affected_records': sum([int(issue.split()[3]) for issue in issues if 'invalid' in issue])
            }
        
        rule = ValidationRule(
            name="referential_integrity",
            description="Check referential integrity constraints",
            validator_func=referential_validator,
            severity='error'
        )
        self.add_rule(rule)
    
    def add_business_rule_validation(self, business_rules: List[Dict[str, Any]]):
        """Add custom business rule validation"""
        def business_rule_validator(df: pd.DataFrame) -> Dict[str, Any]:
            issues = []
            
            for rule in business_rules:
                rule_name = rule.get('name', 'unnamed_rule')
                condition = rule.get('condition', '')
                
                try:
                    violations = df.query(f"not ({condition})")
                    if len(violations) > 0:
                        issues.append(f"Business rule '{rule_name}' violated by {len(violations)} records")
                except Exception as e:
                    issues.append(f"Business rule '{rule_name}' evaluation failed: {str(e)}")
            
            return {
                'passed': len(issues) == 0,
                'message': '; '.join(issues) if issues else f'Business rule validation passed for {len(business_rules)} rules',
                'details': {'issues': issues},
                'affected_records': len(issues)
            }
        
        rule = ValidationRule(
            name="business_rules",
            description="Validate custom business rules",
            validator_func=business_rule_validator,
            severity='warning'
        )
        self.add_rule(rule)
    
    def validate(self, df: pd.DataFrame, dataset_name: str = "unknown") -> ValidationResult:
        """Execute all validation rules"""
        
        self.logger.info(f"Starting validation for dataset: {dataset_name}")
        
        validation_results = []
        total_errors = 0
        total_warnings = 0
        
        for rule in self.rules:
            result = rule.validate(df)
            validation_results.append(result)
            
            if not result['passed']:
                if result['severity'] == 'error':
                    total_errors += 1
                elif result['severity'] == 'warning':
                    total_warnings += 1
        
        # Determine overall status
        if total_errors > 0:
            status = ValidationStatus.FAILED
        elif total_warnings > 0:
            status = ValidationStatus.WARNING
        else:
            status = ValidationStatus.PASSED
        
        # Collect error and warning messages
        errors = [r['message'] for r in validation_results if not r['passed'] and r['severity'] == 'error']
        warnings = [r['message'] for r in validation_results if not r['passed'] and r['severity'] == 'warning']
        
        # Calculate passed/failed records (simplified)
        failed_records = sum([r['affected_records'] for r in validation_results if not r['passed']])
        passed_records = len(df) - failed_records
        
        validation_result = ValidationResult(
            dataset_name=dataset_name,
            validation_time=datetime.now(),
            status=status,
            total_records=len(df),
            passed_records=max(0, passed_records),
            failed_records=failed_records,
            warnings=warnings,
            errors=errors,
            details={'rule_results': validation_results}
        )
        
        self.logger.info(f"Validation complete for {dataset_name}: {status.value}")
        return validation_result

# Example validation configurations
SAMPLE_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer", "minimum": 1},
        "name": {"type": "string"},
        "email": {"type": "string"},
        "age": {"type": "integer", "minimum": 0, "maximum": 150},
        "income": {"type": "number", "minimum": 0},
        "status": {"type": "string"}
    },
    "required": ["id", "name", "email"]
}

SAMPLE_BUSINESS_RULES = [
    {
        "name": "adult_users_only",
        "condition": "age >= 18"
    },
    {
        "name": "valid_email_format",
        "condition": "email.str.contains('@', na=False)"
    },
    {
        "name": "positive_income",
        "condition": "income > 0"
    }
]

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data with some issues
    sample_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5, 5],  # Duplicate ID
        'name': ['Alice', 'Bob', None, 'David', 'Eve', 'Frank'],  # Missing name
        'email': ['alice@example.com', 'bob@example.com', 'invalid-email', 'david@example.com', 'eve@example.com', 'frank@example.com'],
        'age': [25, 30, 17, 35, -5, 40],  # Underage and negative age
        'income': [50000, 60000, 0, 70000, 80000, -1000],  # Zero and negative income
        'status': ['active', 'active', 'inactive', 'active', 'active', 'active']
    })
    
    # Create validator
    validator = DataValidator()
    
    # Add validation rules
    validator.add_schema_validation(SAMPLE_SCHEMA)
    validator.add_completeness_check(['id', 'name', 'email'], threshold=0.9)
    validator.add_uniqueness_check(['id', 'email'])
    validator.add_business_rule_validation(SAMPLE_BUSINESS_RULES)
    
    # Run validation
    result = validator.validate(sample_data, "sample_dataset")
    
    print(f"Validation Status: {result.status.value}")
    print(f"Total Records: {result.total_records}")
    print(f"Passed Records: {result.passed_records}")
    print(f"Failed Records: {result.failed_records}")
    
    if result.errors:
        print("\nErrors:")
        for error in result.errors:
            print(f"  - {error}")
    
    if result.warnings:
        print("\nWarnings:")
        for warning in result.warnings:
            print(f"  - {warning}")
```#
# Step 6: Transactional ETL Pipeline with lakeFS

### Core ETL Pipeline with Transactions

```python
# src/pipeline.py
import lakefs
from lakefs.exceptions import ConflictError, NotFoundException
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import uuid
import json
import traceback
from contextlib import contextmanager

from config.settings import config
from src.models import PipelineRun, PipelineStatus, ValidationResult
from src.extractors.database_extractor import DatabaseExtractor
from src.transformers.base_transformer import TransformationPipeline
from src.validators.data_validator import DataValidator
from src.exceptions import ETLPipelineError, DataValidationError, TransformationError

logger = logging.getLogger(__name__)

class TransactionalETLPipeline:
    """Production-ready ETL pipeline with lakeFS transactions"""
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.client = lakefs.Client(
            host=config.lakefs.host,
            username=config.lakefs.username,
            password=config.lakefs.password
        )
        self.repo = self.client.repositories.get(config.lakefs.repository)
        self.logger = logging.getLogger(f"{__name__}.{pipeline_name}")
        
        # Initialize components
        self.extractor = DatabaseExtractor()
        self.validator = DataValidator()
        
        # Pipeline state
        self.current_run: Optional[PipelineRun] = None
        self.working_branch: Optional[str] = None
    
    @contextmanager
    def pipeline_transaction(self, source_branch: str = 'main'):
        """Context manager for transactional pipeline execution"""
        
        run_id = str(uuid.uuid4())
        branch_name = f"etl-run-{run_id[:8]}"
        
        try:
            # Create working branch
            self.logger.info(f"Creating working branch: {branch_name}")
            working_branch = self.repo.branches.create(branch_name, source_reference=source_branch)
            self.working_branch = branch_name
            
            # Initialize pipeline run
            self.current_run = PipelineRun(
                run_id=run_id,
                pipeline_name=self.pipeline_name,
                start_time=datetime.now(),
                status=PipelineStatus.RUNNING,
                source_commit=self.repo.branches.get(source_branch).head.id
            )
            
            # Save run metadata
            self._save_run_metadata(working_branch)
            
            yield working_branch
            
            # If we get here, the pipeline succeeded
            self.current_run.status = PipelineStatus.SUCCESS
            self.current_run.end_time = datetime.now()
            self.current_run.target_commit = working_branch.head.id
            
            # Final commit with run summary
            self._save_run_metadata(working_branch)
            working_branch.commits.create(
                message=f"ETL pipeline {self.pipeline_name} completed successfully",
                metadata={
                    'pipeline_name': self.pipeline_name,
                    'run_id': run_id,
                    'status': 'success',
                    'records_processed': str(self.current_run.records_processed),
                    'duration_seconds': str((self.current_run.end_time - self.current_run.start_time).total_seconds())
                }
            )
            
            self.logger.info(f"Pipeline {self.pipeline_name} completed successfully")
            
        except Exception as e:
            # Pipeline failed, update status
            if self.current_run:
                self.current_run.status = PipelineStatus.FAILED
                self.current_run.end_time = datetime.now()
                self.current_run.error_message = str(e)
                
                # Save failure metadata
                try:
                    if self.working_branch:
                        branch = self.repo.branches.get(self.working_branch)
                        self._save_run_metadata(branch)
                        branch.commits.create(
                            message=f"ETL pipeline {self.pipeline_name} failed: {str(e)[:100]}",
                            metadata={
                                'pipeline_name': self.pipeline_name,
                                'run_id': run_id,
                                'status': 'failed',
                                'error': str(e)[:500]
                            }
                        )
                except Exception as save_error:
                    self.logger.error(f"Failed to save error metadata: {save_error}")
            
            self.logger.error(f"Pipeline {self.pipeline_name} failed: {e}")
            self.logger.debug(traceback.format_exc())
            raise
        
        finally:
            # Cleanup
            self.working_branch = None
    
    def _save_run_metadata(self, branch):
        """Save pipeline run metadata to lakeFS"""
        if self.current_run:
            metadata_json = json.dumps(self.current_run.to_dict(), indent=2)
            branch.objects.upload(
                path=f"logs/pipeline_runs/{self.current_run.run_id}.json",
                data=metadata_json.encode(),
                content_type='application/json'
            )
    
    def extract_data(self, branch, extraction_config: Dict[str, Any]) -> pd.DataFrame:
        """Extract data from configured sources"""
        
        self.logger.info("Starting data extraction")
        
        extraction_type = extraction_config.get('type', 'full')
        table_name = extraction_config.get('table_name')
        
        if not table_name:
            raise ETLPipelineError("Table name not specified in extraction config")
        
        try:
            if extraction_type == 'incremental':
                # Get last extraction timestamp
                last_extracted = self._get_last_extraction_time(table_name)
                timestamp_column = extraction_config.get('timestamp_column', 'updated_at')
                
                data_chunks = list(self.extractor.extract_incremental(
                    table_name=table_name,
                    timestamp_column=timestamp_column,
                    last_extracted=last_extracted,
                    chunk_size=config.pipeline.batch_size
                ))
            else:
                # Full extraction
                data_chunks = list(self.extractor.extract_full_table(
                    table_name=table_name,
                    chunk_size=config.pipeline.batch_size
                ))
            
            if not data_chunks:
                self.logger.warning("No data extracted")
                return pd.DataFrame()
            
            # Combine chunks
            combined_df = pd.concat(data_chunks, ignore_index=True)
            
            # Save raw data to lakeFS
            raw_data_path = f"raw_data/{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            self._save_dataframe_to_lakefs(branch, combined_df, raw_data_path)
            
            self.logger.info(f"Extracted {len(combined_df)} records from {table_name}")
            
            if self.current_run:
                self.current_run.records_processed += len(combined_df)
            
            return combined_df
            
        except Exception as e:
            raise ETLPipelineError(f"Data extraction failed: {str(e)}") from e
    
    def transform_data(self, branch, df: pd.DataFrame, transformation_pipeline: TransformationPipeline) -> pd.DataFrame:
        """Apply transformations to extracted data"""
        
        self.logger.info("Starting data transformation")
        
        try:
            transformed_df = transformation_pipeline.execute(df)
            
            # Save transformed data
            transformed_data_path = f"processed_data/transformed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            self._save_dataframe_to_lakefs(branch, transformed_df, transformed_data_path)
            
            self.logger.info(f"Transformed data: {len(df)} -> {len(transformed_df)} records")
            return transformed_df
            
        except Exception as e:
            raise TransformationError(f"Data transformation failed: {str(e)}") from e
    
    def validate_data(self, branch, df: pd.DataFrame, dataset_name: str) -> ValidationResult:
        """Validate transformed data"""
        
        self.logger.info("Starting data validation")
        
        try:
            validation_result = self.validator.validate(df, dataset_name)
            
            # Save validation results
            validation_path = f"validation_results/{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            validation_json = json.dumps(validation_result.to_dict(), indent=2)
            branch.objects.upload(
                path=validation_path,
                data=validation_json.encode(),
                content_type='application/json'
            )
            
            # Handle validation failures
            if validation_result.status.value == 'failed':
                # Save failed records for analysis
                if validation_result.failed_records > 0:
                    failed_data_path = f"failed_data/{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                    # In practice, you'd identify and save the actual failed records
                    self.logger.warning(f"Validation failed: {validation_result.failed_records} records failed validation")
                
                if not config.pipeline.validation_enabled:
                    self.logger.warning("Validation failed but continuing due to configuration")
                else:
                    raise DataValidationError(f"Data validation failed: {'; '.join(validation_result.errors)}")
            
            self.logger.info(f"Data validation completed: {validation_result.status.value}")
            
            if self.current_run:
                self.current_run.records_failed += validation_result.failed_records
            
            return validation_result
            
        except Exception as e:
            if isinstance(e, DataValidationError):
                raise
            raise DataValidationError(f"Data validation process failed: {str(e)}") from e
    
    def load_data(self, branch, df: pd.DataFrame, load_config: Dict[str, Any]) -> bool:
        """Load validated data to final destination"""
        
        self.logger.info("Starting data loading")
        
        try:
            # Save to final location in lakeFS
            final_data_path = load_config.get('path', f"validated_data/final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
            self._save_dataframe_to_lakefs(branch, df, final_data_path)
            
            # Optional: Load to external systems (database, data warehouse, etc.)
            external_targets = load_config.get('external_targets', [])
            for target in external_targets:
                self._load_to_external_target(df, target)
            
            self.logger.info(f"Data loading completed: {len(df)} records")
            return True
            
        except Exception as e:
            raise ETLPipelineError(f"Data loading failed: {str(e)}") from e
    
    def _save_dataframe_to_lakefs(self, branch, df: pd.DataFrame, path: str):
        """Save DataFrame to lakeFS as Parquet"""
        import io
        
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        
        branch.objects.upload(
            path=path,
            data=buffer.getvalue(),
            content_type='application/octet-stream'
        )
        
        self.logger.debug(f"Saved DataFrame to {path}: {len(df)} records")
    
    def _load_to_external_target(self, df: pd.DataFrame, target_config: Dict[str, Any]):
        """Load data to external target (database, API, etc.)"""
        target_type = target_config.get('type')
        
        if target_type == 'database':
            # Example database loading
            table_name = target_config.get('table_name')
            if_exists = target_config.get('if_exists', 'append')
            
            df.to_sql(
                table_name,
                self.extractor.engine,
                if_exists=if_exists,
                index=False,
                chunksize=config.pipeline.batch_size
            )
            
            self.logger.info(f"Loaded {len(df)} records to database table {table_name}")
        
        elif target_type == 'api':
            # Example API loading
            api_url = target_config.get('url')
            # Implementation would depend on specific API requirements
            self.logger.info(f"Would load {len(df)} records to API endpoint {api_url}")
        
        else:
            self.logger.warning(f"Unknown target type: {target_type}")
    
    def _get_last_extraction_time(self, table_name: str) -> Optional[datetime]:
        """Get timestamp of last successful extraction"""
        try:
            # Look for checkpoint files
            main_branch = self.repo.branches.get('main')
            checkpoint_path = f"checkpoints/{table_name}_last_extraction.json"
            
            checkpoint_obj = main_branch.objects.get(checkpoint_path)
            checkpoint_data = json.loads(checkpoint_obj.reader().read().decode())
            
            return datetime.fromisoformat(checkpoint_data['last_extraction_time'])
            
        except NotFoundException:
            # No previous extraction found
            self.logger.info(f"No previous extraction checkpoint found for {table_name}")
            return None
        except Exception as e:
            self.logger.warning(f"Error reading extraction checkpoint: {e}")
            return None
    
    def _save_extraction_checkpoint(self, branch, table_name: str, extraction_time: datetime):
        """Save extraction checkpoint"""
        checkpoint_data = {
            'table_name': table_name,
            'last_extraction_time': extraction_time.isoformat(),
            'pipeline_run_id': self.current_run.run_id if self.current_run else None
        }
        
        checkpoint_path = f"checkpoints/{table_name}_last_extraction.json"
        branch.objects.upload(
            path=checkpoint_path,
            data=json.dumps(checkpoint_data, indent=2).encode(),
            content_type='application/json'
        )
    
    def run_pipeline(
        self,
        extraction_config: Dict[str, Any],
        transformation_pipeline: TransformationPipeline,
        load_config: Dict[str, Any],
        source_branch: str = 'main'
    ) -> PipelineRun:
        """Execute complete ETL pipeline"""
        
        self.logger.info(f"Starting ETL pipeline: {self.pipeline_name}")
        
        with self.pipeline_transaction(source_branch) as working_branch:
            
            # Extract
            extracted_df = self.extract_data(working_branch, extraction_config)
            
            if extracted_df.empty:
                self.logger.info("No data to process, pipeline completed")
                return self.current_run
            
            # Transform
            transformed_df = self.transform_data(working_branch, extracted_df, transformation_pipeline)
            
            # Validate
            validation_result = self.validate_data(working_branch, transformed_df, extraction_config.get('table_name', 'unknown'))
            
            # Load
            self.load_data(working_branch, transformed_df, load_config)
            
            # Save extraction checkpoint
            self._save_extraction_checkpoint(working_branch, extraction_config.get('table_name'), datetime.now())
            
            self.logger.info(f"ETL pipeline {self.pipeline_name} completed successfully")
        
        return self.current_run
    
    def merge_to_target(self, target_branch: str = 'main', delete_working_branch: bool = True) -> str:
        """Merge working branch to target branch"""
        
        if not self.current_run or not self.current_run.target_commit:
            raise ETLPipelineError("No successful pipeline run to merge")
        
        working_branch_name = f"etl-run-{self.current_run.run_id[:8]}"
        
        try:
            target_branch_obj = self.repo.branches.get(target_branch)
            
            merge_result = target_branch_obj.merge(
                source_reference=working_branch_name,
                message=f"Merge ETL pipeline {self.pipeline_name} results",
                metadata={
                    'pipeline_name': self.pipeline_name,
                    'run_id': self.current_run.run_id,
                    'records_processed': str(self.current_run.records_processed)
                }
            )
            
            self.logger.info(f"Merged {working_branch_name} to {target_branch}: {merge_result.id}")
            
            # Optionally delete working branch
            if delete_working_branch:
                try:
                    self.repo.branches.delete(working_branch_name)
                    self.logger.info(f"Deleted working branch: {working_branch_name}")
                except Exception as e:
                    self.logger.warning(f"Failed to delete working branch: {e}")
            
            return merge_result.id
            
        except ConflictError as e:
            self.logger.error(f"Merge conflict: {e}")
            raise ETLPipelineError(f"Merge conflict when merging to {target_branch}") from e
    
    def cleanup(self):
        """Cleanup resources"""
        if self.extractor:
            self.extractor.close()

# Example pipeline configuration
SAMPLE_EXTRACTION_CONFIG = {
    'type': 'incremental',  # or 'full'
    'table_name': 'users',
    'timestamp_column': 'updated_at'
}

SAMPLE_LOAD_CONFIG = {
    'path': 'validated_data/users.parquet',
    'external_targets': [
        {
            'type': 'database',
            'table_name': 'processed_users',
            'if_exists': 'replace'
        }
    ]
}

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    from src.transformers.base_transformer import (
        DataCleaningTransformer, 
        BusinessLogicTransformer,
        TransformationPipeline,
        SAMPLE_CLEANING_RULES,
        SAMPLE_BUSINESS_RULES
    )
    from src.validators.data_validator import DataValidator, SAMPLE_SCHEMA, SAMPLE_BUSINESS_RULES as VALIDATION_RULES
    
    # Create pipeline
    pipeline = TransactionalETLPipeline("user_processing_pipeline")
    
    # Setup validation
    pipeline.validator.add_schema_validation(SAMPLE_SCHEMA)
    pipeline.validator.add_completeness_check(['id', 'name', 'email'])
    pipeline.validator.add_uniqueness_check(['id', 'email'])
    pipeline.validator.add_business_rule_validation(VALIDATION_RULES)
    
    # Setup transformation pipeline
    transformers = [
        DataCleaningTransformer(SAMPLE_CLEANING_RULES),
        BusinessLogicTransformer(SAMPLE_BUSINESS_RULES)
    ]
    transformation_pipeline = TransformationPipeline(transformers)
    
    try:
        # Run pipeline
        run_result = pipeline.run_pipeline(
            extraction_config=SAMPLE_EXTRACTION_CONFIG,
            transformation_pipeline=transformation_pipeline,
            load_config=SAMPLE_LOAD_CONFIG
        )
        
        print(f"Pipeline completed: {run_result.status.value}")
        print(f"Records processed: {run_result.records_processed}")
        
        # Merge to main if successful
        if run_result.status == PipelineStatus.SUCCESS:
            merge_commit = pipeline.merge_to_target('main')
            print(f"Merged to main: {merge_commit}")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
    
    finally:
        pipeline.cleanup()
```## Step 7: 
Error Handling and Recovery

### Comprehensive Error Handling System

```python
# src/exceptions.py
class ETLPipelineError(Exception):
    """Base exception for ETL pipeline errors"""
    pass

class DataExtractionError(ETLPipelineError):
    """Raised when data extraction fails"""
    pass

class TransformationError(ETLPipelineError):
    """Raised when data transformation fails"""
    pass

class DataValidationError(ETLPipelineError):
    """Raised when data validation fails"""
    pass

class DataLoadingError(ETLPipelineError):
    """Raised when data loading fails"""
    pass

# src/error_handler.py
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime
import traceback
import json
from enum import Enum

class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ErrorHandler:
    """Centralized error handling and recovery system"""
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.logger = logging.getLogger(f"{__name__}.{pipeline_name}")
        self.error_log = []
    
    def handle_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        recovery_action: Optional[Callable] = None
    ) -> bool:
        """Handle pipeline errors with optional recovery"""
        
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'pipeline_name': self.pipeline_name,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'severity': severity.value,
            'context': context,
            'traceback': traceback.format_exc()
        }
        
        self.error_log.append(error_info)
        
        # Log error based on severity
        if severity == ErrorSeverity.CRITICAL:
            self.logger.critical(f"Critical error in {self.pipeline_name}: {error}")
        elif severity == ErrorSeverity.HIGH:
            self.logger.error(f"High severity error in {self.pipeline_name}: {error}")
        elif severity == ErrorSeverity.MEDIUM:
            self.logger.warning(f"Medium severity error in {self.pipeline_name}: {error}")
        else:
            self.logger.info(f"Low severity error in {self.pipeline_name}: {error}")
        
        # Attempt recovery if provided
        if recovery_action:
            try:
                self.logger.info("Attempting error recovery...")
                recovery_result = recovery_action()
                if recovery_result:
                    self.logger.info("Error recovery successful")
                    return True
                else:
                    self.logger.warning("Error recovery failed")
            except Exception as recovery_error:
                self.logger.error(f"Error recovery failed with exception: {recovery_error}")
        
        return False
    
    def save_error_log(self, branch, run_id: str):
        """Save error log to lakeFS"""
        if self.error_log:
            error_log_path = f"logs/errors/{run_id}_errors.json"
            error_log_json = json.dumps(self.error_log, indent=2)
            
            branch.objects.upload(
                path=error_log_path,
                data=error_log_json.encode(),
                content_type='application/json'
            )
            
            self.logger.info(f"Error log saved to {error_log_path}")

# src/retry_handler.py
import time
import random
from typing import Callable, Any, Optional
from functools import wraps

class RetryHandler:
    """Retry mechanism with exponential backoff"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.logger = logging.getLogger(__name__)
    
    def retry_with_backoff(
        self,
        func: Callable,
        *args,
        retryable_exceptions: tuple = (Exception,),
        **kwargs
    ) -> Any:
        """Execute function with retry and exponential backoff"""
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            
            except retryable_exceptions as e:
                last_exception = e
                
                if attempt == self.max_retries:
                    self.logger.error(f"Function {func.__name__} failed after {self.max_retries} retries")
                    raise e
                
                # Calculate delay with jitter
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                jitter = random.uniform(0, 0.1) * delay
                total_delay = delay + jitter
                
                self.logger.warning(
                    f"Function {func.__name__} failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}. "
                    f"Retrying in {total_delay:.2f} seconds..."
                )
                
                time.sleep(total_delay)
        
        # This should never be reached, but just in case
        raise last_exception

def retry_on_failure(max_retries: int = 3, base_delay: float = 1.0, retryable_exceptions: tuple = (Exception,)):
    """Decorator for automatic retry with exponential backoff"""
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_handler = RetryHandler(max_retries, base_delay)
            return retry_handler.retry_with_backoff(
                func, *args, retryable_exceptions=retryable_exceptions, **kwargs
            )
        return wrapper
    return decorator

# Enhanced pipeline with error handling
class RobustETLPipeline(TransactionalETLPipeline):
    """ETL Pipeline with comprehensive error handling and recovery"""
    
    def __init__(self, pipeline_name: str):
        super().__init__(pipeline_name)
        self.error_handler = ErrorHandler(pipeline_name)
        self.retry_handler = RetryHandler(
            max_retries=config.pipeline.max_retries,
            base_delay=1.0,
            max_delay=30.0
        )
    
    @retry_on_failure(max_retries=3, retryable_exceptions=(DataExtractionError,))
    def extract_data_with_retry(self, branch, extraction_config: Dict[str, Any]) -> pd.DataFrame:
        """Extract data with automatic retry on failure"""
        try:
            return super().extract_data(branch, extraction_config)
        except Exception as e:
            # Convert to specific exception type for retry logic
            raise DataExtractionError(f"Data extraction failed: {str(e)}") from e
    
    def transform_data_with_recovery(self, branch, df: pd.DataFrame, transformation_pipeline) -> pd.DataFrame:
        """Transform data with error recovery"""
        
        def recovery_action():
            """Recovery action for transformation failures"""
            self.logger.info("Attempting transformation recovery with simplified rules")
            
            # Create a simplified transformation pipeline
            from src.transformers.base_transformer import DataCleaningTransformer
            
            simplified_rules = {
                'remove_duplicates': True,
                'null_handling': {'*': 'drop'},  # Drop any null values
                'type_conversions': {}  # Skip type conversions
            }
            
            simplified_transformer = DataCleaningTransformer(simplified_rules)
            
            try:
                return simplified_transformer.execute(df)
            except Exception:
                return df  # Return original data if even simplified transformation fails
        
        try:
            return super().transform_data(branch, df, transformation_pipeline)
        
        except Exception as e:
            context = {
                'step': 'transformation',
                'input_records': len(df),
                'transformation_pipeline': str(transformation_pipeline)
            }
            
            recovery_successful = self.error_handler.handle_error(
                error=e,
                context=context,
                severity=ErrorSeverity.HIGH,
                recovery_action=recovery_action
            )
            
            if recovery_successful:
                return recovery_action()
            else:
                raise TransformationError(f"Transformation failed and recovery unsuccessful: {str(e)}") from e
    
    def validate_data_with_fallback(self, branch, df: pd.DataFrame, dataset_name: str):
        """Validate data with fallback to warnings-only mode"""
        
        def recovery_action():
            """Recovery action for validation failures"""
            self.logger.info("Attempting validation recovery with relaxed rules")
            
            # Create a more lenient validator
            lenient_validator = DataValidator()
            
            # Add only critical validations
            lenient_validator.add_completeness_check(['id'], threshold=0.5)  # Lower threshold
            
            try:
                return lenient_validator.validate(df, f"{dataset_name}_lenient")
            except Exception:
                # If even lenient validation fails, create a minimal passing result
                from src.models import ValidationResult, ValidationStatus
                return ValidationResult(
                    dataset_name=dataset_name,
                    validation_time=datetime.now(),
                    status=ValidationStatus.WARNING,
                    total_records=len(df),
                    passed_records=len(df),
                    failed_records=0,
                    warnings=["Validation failed, using fallback mode"],
                    errors=[]
                )
        
        try:
            return super().validate_data(branch, df, dataset_name)
        
        except DataValidationError as e:
            context = {
                'step': 'validation',
                'dataset_name': dataset_name,
                'record_count': len(df)
            }
            
            recovery_successful = self.error_handler.handle_error(
                error=e,
                context=context,
                severity=ErrorSeverity.MEDIUM,
                recovery_action=recovery_action
            )
            
            if recovery_successful:
                return recovery_action()
            else:
                raise e
    
    def run_pipeline_with_recovery(
        self,
        extraction_config: Dict[str, Any],
        transformation_pipeline,
        load_config: Dict[str, Any],
        source_branch: str = 'main'
    ):
        """Run pipeline with comprehensive error handling"""
        
        try:
            with self.pipeline_transaction(source_branch) as working_branch:
                
                # Extract with retry
                extracted_df = self.extract_data_with_retry(working_branch, extraction_config)
                
                if extracted_df.empty:
                    self.logger.info("No data to process, pipeline completed")
                    return self.current_run
                
                # Transform with recovery
                transformed_df = self.transform_data_with_recovery(working_branch, extracted_df, transformation_pipeline)
                
                # Validate with fallback
                validation_result = self.validate_data_with_fallback(
                    working_branch, 
                    transformed_df, 
                    extraction_config.get('table_name', 'unknown')
                )
                
                # Load with retry
                self.retry_handler.retry_with_backoff(
                    self.load_data,
                    working_branch,
                    transformed_df,
                    load_config,
                    retryable_exceptions=(DataLoadingError,)
                )
                
                # Save extraction checkpoint
                self._save_extraction_checkpoint(
                    working_branch, 
                    extraction_config.get('table_name'), 
                    datetime.now()
                )
                
                self.logger.info(f"ETL pipeline {self.pipeline_name} completed successfully")
        
        except Exception as e:
            # Final error handling
            context = {
                'step': 'pipeline_execution',
                'extraction_config': extraction_config,
                'load_config': load_config
            }
            
            self.error_handler.handle_error(
                error=e,
                context=context,
                severity=ErrorSeverity.CRITICAL
            )
            
            raise
        
        finally:
            # Always save error log
            if self.working_branch and self.current_run:
                try:
                    branch = self.repo.branches.get(self.working_branch)
                    self.error_handler.save_error_log(branch, self.current_run.run_id)
                except Exception as log_error:
                    self.logger.error(f"Failed to save error log: {log_error}")
        
        return self.current_run

# Example usage with error handling
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create robust pipeline
    pipeline = RobustETLPipeline("robust_user_processing")
    
    # Setup components (same as before)
    from src.transformers.base_transformer import (
        DataCleaningTransformer, 
        BusinessLogicTransformer,
        TransformationPipeline,
        SAMPLE_CLEANING_RULES,
        SAMPLE_BUSINESS_RULES
    )
    
    pipeline.validator.add_schema_validation(SAMPLE_SCHEMA)
    pipeline.validator.add_completeness_check(['id', 'name', 'email'])
    pipeline.validator.add_uniqueness_check(['id', 'email'])
    
    transformers = [
        DataCleaningTransformer(SAMPLE_CLEANING_RULES),
        BusinessLogicTransformer(SAMPLE_BUSINESS_RULES)
    ]
    transformation_pipeline = TransformationPipeline(transformers)
    
    try:
        # Run robust pipeline
        run_result = pipeline.run_pipeline_with_recovery(
            extraction_config=SAMPLE_EXTRACTION_CONFIG,
            transformation_pipeline=transformation_pipeline,
            load_config=SAMPLE_LOAD_CONFIG
        )
        
        print(f"Robust pipeline completed: {run_result.status.value}")
        print(f"Records processed: {run_result.records_processed}")
        print(f"Errors encountered: {len(pipeline.error_handler.error_log)}")
        
    except Exception as e:
        print(f"Pipeline failed despite error handling: {e}")
    
    finally:
        pipeline.cleanup()
```

## Step 8: Monitoring and Alerting

### Pipeline Monitoring System

```python
# src/monitoring.py
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
from dataclasses import dataclass
from enum import Enum
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class PipelineMetrics:
    """Pipeline execution metrics"""
    pipeline_name: str
    run_id: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: Optional[float]
    records_processed: int
    records_failed: int
    success_rate: float
    error_count: int
    warning_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'pipeline_name': self.pipeline_name,
            'run_id': self.run_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'records_processed': self.records_processed,
            'records_failed': self.records_failed,
            'success_rate': self.success_rate,
            'error_count': self.error_count,
            'warning_count': self.warning_count
        }

class PipelineMonitor:
    """Monitor pipeline execution and send alerts"""
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.logger = logging.getLogger(f"{__name__}.{pipeline_name}")
        self.metrics_history: List[PipelineMetrics] = []
    
    def record_metrics(self, pipeline_run) -> PipelineMetrics:
        """Record metrics from pipeline run"""
        
        duration = None
        if pipeline_run.end_time and pipeline_run.start_time:
            duration = (pipeline_run.end_time - pipeline_run.start_time).total_seconds()
        
        success_rate = 0.0
        if pipeline_run.records_processed > 0:
            success_rate = (pipeline_run.records_processed - pipeline_run.records_failed) / pipeline_run.records_processed
        
        metrics = PipelineMetrics(
            pipeline_name=self.pipeline_name,
            run_id=pipeline_run.run_id,
            start_time=pipeline_run.start_time,
            end_time=pipeline_run.end_time,
            duration_seconds=duration,
            records_processed=pipeline_run.records_processed,
            records_failed=pipeline_run.records_failed,
            success_rate=success_rate,
            error_count=1 if pipeline_run.status.value == 'failed' else 0,
            warning_count=0  # Would be populated from validation results
        )
        
        self.metrics_history.append(metrics)
        return metrics
    
    def check_sla_violations(self, metrics: PipelineMetrics) -> List[Dict[str, Any]]:
        """Check for SLA violations"""
        violations = []
        
        # Duration SLA (example: pipeline should complete within 30 minutes)
        max_duration = 30 * 60  # 30 minutes in seconds
        if metrics.duration_seconds and metrics.duration_seconds > max_duration:
            violations.append({
                'type': 'duration_sla',
                'message': f"Pipeline duration {metrics.duration_seconds:.0f}s exceeds SLA of {max_duration}s",
                'severity': AlertLevel.WARNING
            })
        
        # Success rate SLA (example: should process 95% of records successfully)
        min_success_rate = 0.95
        if metrics.success_rate < min_success_rate:
            violations.append({
                'type': 'success_rate_sla',
                'message': f"Success rate {metrics.success_rate:.2%} below SLA of {min_success_rate:.2%}",
                'severity': AlertLevel.ERROR
            })
        
        # Error count SLA (example: no errors allowed)
        if metrics.error_count > 0:
            violations.append({
                'type': 'error_count_sla',
                'message': f"Pipeline failed with {metrics.error_count} errors",
                'severity': AlertLevel.CRITICAL
            })
        
        return violations
    
    def generate_report(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """Generate pipeline performance report"""
        
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        recent_metrics = [m for m in self.metrics_history if m.start_time >= cutoff_time]
        
        if not recent_metrics:
            return {
                'pipeline_name': self.pipeline_name,
                'time_window_hours': time_window_hours,
                'total_runs': 0,
                'message': 'No pipeline runs in the specified time window'
            }
        
        # Calculate aggregated metrics
        total_runs = len(recent_metrics)
        successful_runs = len([m for m in recent_metrics if m.error_count == 0])
        total_records_processed = sum(m.records_processed for m in recent_metrics)
        total_records_failed = sum(m.records_failed for m in recent_metrics)
        
        avg_duration = None
        completed_runs = [m for m in recent_metrics if m.duration_seconds is not None]
        if completed_runs:
            avg_duration = sum(m.duration_seconds for m in completed_runs) / len(completed_runs)
        
        overall_success_rate = successful_runs / total_runs if total_runs > 0 else 0
        
        return {
            'pipeline_name': self.pipeline_name,
            'time_window_hours': time_window_hours,
            'report_generated': datetime.now().isoformat(),
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'failed_runs': total_runs - successful_runs,
            'overall_success_rate': overall_success_rate,
            'total_records_processed': total_records_processed,
            'total_records_failed': total_records_failed,
            'average_duration_seconds': avg_duration,
            'recent_runs': [m.to_dict() for m in recent_metrics[-5:]]  # Last 5 runs
        }
    
    def save_metrics(self, branch, metrics: PipelineMetrics):
        """Save metrics to lakeFS"""
        metrics_path = f"monitoring/metrics/{metrics.run_id}_metrics.json"
        metrics_json = json.dumps(metrics.to_dict(), indent=2)
        
        branch.objects.upload(
            path=metrics_path,
            data=metrics_json.encode(),
            content_type='application/json'
        )
        
        self.logger.info(f"Metrics saved to {metrics_path}")

class AlertManager:
    """Manage pipeline alerts and notifications"""
    
    def __init__(self, smtp_config: Optional[Dict[str, Any]] = None):
        self.smtp_config = smtp_config or {}
        self.logger = logging.getLogger(__name__)
    
    def send_alert(self, alert_level: AlertLevel, subject: str, message: str, recipients: List[str]):
        """Send alert notification"""
        
        # Log alert
        log_method = {
            AlertLevel.INFO: self.logger.info,
            AlertLevel.WARNING: self.logger.warning,
            AlertLevel.ERROR: self.logger.error,
            AlertLevel.CRITICAL: self.logger.critical
        }.get(alert_level, self.logger.info)
        
        log_method(f"ALERT [{alert_level.value.upper()}]: {subject} - {message}")
        
        # Send email if configured
        if self.smtp_config and recipients:
            try:
                self._send_email_alert(alert_level, subject, message, recipients)
            except Exception as e:
                self.logger.error(f"Failed to send email alert: {e}")
    
    def _send_email_alert(self, alert_level: AlertLevel, subject: str, message: str, recipients: List[str]):
        """Send email alert"""
        
        smtp_server = self.smtp_config.get('server', 'localhost')
        smtp_port = self.smtp_config.get('port', 587)
        username = self.smtp_config.get('username')
        password = self.smtp_config.get('password')
        sender = self.smtp_config.get('sender', 'etl-pipeline@company.com')
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = f"[{alert_level.value.upper()}] {subject}"
        
        # Add body
        body = f"""
        Alert Level: {alert_level.value.upper()}
        Subject: {subject}
        
        Message:
        {message}
        
        Generated at: {datetime.now().isoformat()}
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            if username and password:
                server.starttls()
                server.login(username, password)
            
            server.send_message(msg)
        
        self.logger.info(f"Email alert sent to {len(recipients)} recipients")

# Enhanced pipeline with monitoring
class MonitoredETLPipeline(RobustETLPipeline):
    """ETL Pipeline with comprehensive monitoring and alerting"""
    
    def __init__(self, pipeline_name: str, alert_recipients: List[str] = None):
        super().__init__(pipeline_name)
        self.monitor = PipelineMonitor(pipeline_name)
        self.alert_manager = AlertManager()
        self.alert_recipients = alert_recipients or []
    
    def run_monitored_pipeline(
        self,
        extraction_config: Dict[str, Any],
        transformation_pipeline,
        load_config: Dict[str, Any],
        source_branch: str = 'main'
    ):
        """Run pipeline with monitoring and alerting"""
        
        try:
            # Run pipeline
            run_result = self.run_pipeline_with_recovery(
                extraction_config, transformation_pipeline, load_config, source_branch
            )
            
            # Record metrics
            metrics = self.monitor.record_metrics(run_result)
            
            # Save metrics to lakeFS
            if self.working_branch:
                branch = self.repo.branches.get(self.working_branch)
                self.monitor.save_metrics(branch, metrics)
            
            # Check for SLA violations
            violations = self.monitor.check_sla_violations(metrics)
            
            # Send alerts for violations
            for violation in violations:
                self.alert_manager.send_alert(
                    alert_level=violation['severity'],
                    subject=f"Pipeline SLA Violation: {self.pipeline_name}",
                    message=violation['message'],
                    recipients=self.alert_recipients
                )
            
            # Send success notification for critical pipelines
            if run_result.status.value == 'success' and not violations:
                self.alert_manager.send_alert(
                    alert_level=AlertLevel.INFO,
                    subject=f"Pipeline Success: {self.pipeline_name}",
                    message=f"Pipeline completed successfully. Processed {metrics.records_processed} records in {metrics.duration_seconds:.0f} seconds.",
                    recipients=self.alert_recipients
                )
            
            return run_result
            
        except Exception as e:
            # Send critical failure alert
            self.alert_manager.send_alert(
                alert_level=AlertLevel.CRITICAL,
                subject=f"Pipeline Failure: {self.pipeline_name}",
                message=f"Pipeline failed with error: {str(e)}",
                recipients=self.alert_recipients
            )
            raise
    
    def generate_daily_report(self) -> Dict[str, Any]:
        """Generate and send daily pipeline report"""
        
        report = self.monitor.generate_report(time_window_hours=24)
        
        # Send report via email
        if self.alert_recipients:
            report_message = f"""
            Daily Pipeline Report: {self.pipeline_name}
            
            Summary:
            - Total Runs: {report['total_runs']}
            - Successful Runs: {report['successful_runs']}
            - Failed Runs: {report['failed_runs']}
            - Overall Success Rate: {report['overall_success_rate']:.2%}
            - Total Records Processed: {report['total_records_processed']:,}
            - Total Records Failed: {report['total_records_failed']:,}
            - Average Duration: {report['average_duration_seconds']:.0f} seconds
            
            For detailed metrics, check the pipeline monitoring dashboard.
            """
            
            self.alert_manager.send_alert(
                alert_level=AlertLevel.INFO,
                subject=f"Daily Report: {self.pipeline_name}",
                message=report_message,
                recipients=self.alert_recipients
            )
        
        return report

# Example usage with monitoring
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create monitored pipeline
    pipeline = MonitoredETLPipeline(
        "monitored_user_processing",
        alert_recipients=["data-team@company.com", "ops-team@company.com"]
    )
    
    # Setup components
    from src.transformers.base_transformer import (
        DataCleaningTransformer, 
        BusinessLogicTransformer,
        TransformationPipeline,
        SAMPLE_CLEANING_RULES,
        SAMPLE_BUSINESS_RULES
    )
    
    pipeline.validator.add_schema_validation(SAMPLE_SCHEMA)
    pipeline.validator.add_completeness_check(['id', 'name', 'email'])
    pipeline.validator.add_uniqueness_check(['id', 'email'])
    
    transformers = [
        DataCleaningTransformer(SAMPLE_CLEANING_RULES),
        BusinessLogicTransformer(SAMPLE_BUSINESS_RULES)
    ]
    transformation_pipeline = TransformationPipeline(transformers)
    
    try:
        # Run monitored pipeline
        run_result = pipeline.run_monitored_pipeline(
            extraction_config=SAMPLE_EXTRACTION_CONFIG,
            transformation_pipeline=transformation_pipeline,
            load_config=SAMPLE_LOAD_CONFIG
        )
        
        print(f"Monitored pipeline completed: {run_result.status.value}")
        
        # Generate daily report
        daily_report = pipeline.generate_daily_report()
        print("Daily report generated and sent")
        
    except Exception as e:
        print(f"Monitored pipeline failed: {e}")
    
    finally:
        pipeline.cleanup()
```##
 Step 9: Production Deployment and CI/CD

### Production Deployment Configuration

```python
# deployment/docker/Dockerfile
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY deployment/scripts/ ./scripts/

# Create non-root user
RUN useradd -m -u 1000 etluser && chown -R etluser:etluser /app
USER etluser

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python scripts/health_check.py

# Default command
CMD ["python", "scripts/run_pipeline.py"]
```

```yaml
# deployment/kubernetes/etl-pipeline.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: etl-pipeline
  labels:
    app: etl-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app: etl-pipeline
  template:
    metadata:
      labels:
        app: etl-pipeline
    spec:
      containers:
      - name: etl-pipeline
        image: your-registry/etl-pipeline:latest
        env:
        - name: LAKEFS_HOST
          valueFrom:
            secretKeyRef:
              name: lakefs-credentials
              key: host
        - name: LAKEFS_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: lakefs-credentials
              key: access-key
        - name: LAKEFS_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: lakefs-credentials
              key: secret-key
        - name: DB_HOST
          valueFrom:
            secretKeyRef:
              name: database-credentials
              key: host
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: database-credentials
              key: password
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
      volumes:
      - name: config-volume
        configMap:
          name: etl-pipeline-config
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: etl-pipeline-config
data:
  pipeline.yaml: |
    batch_size: 1000
    max_retries: 3
    timeout_seconds: 300
    validation_enabled: true
    monitoring_enabled: true
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: etl-pipeline-schedule
spec:
  schedule: "0 2 * * *"  # Run daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: etl-pipeline
            image: your-registry/etl-pipeline:latest
            command: ["python", "scripts/run_scheduled_pipeline.py"]
            env:
            - name: PIPELINE_NAME
              value: "daily_user_processing"
          restartPolicy: OnFailure
```

### CI/CD Pipeline Configuration

```yaml
# .github/workflows/etl-pipeline.yml
name: ETL Pipeline CI/CD

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}/etl-pipeline

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:13
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test_db
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
      
      lakefs:
        image: treeverse/lakefs:latest
        env:
          LAKEFS_AUTH_ENCRYPT_SECRET_KEY: some-secret
          LAKEFS_DATABASE_TYPE: local
          LAKEFS_BLOCKSTORE_TYPE: local
        ports:
          - 8000:8000
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install -r requirements-test.txt
    
    - name: Wait for services
      run: |
        sleep 30  # Wait for services to be ready
    
    - name: Run unit tests
      run: |
        python -m pytest tests/unit/ -v --cov=src --cov-report=xml
      env:
        LAKEFS_HOST: http://localhost:8000
        LAKEFS_ACCESS_KEY: AKIAIOSFODNN7EXAMPLE
        LAKEFS_SECRET_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
        DB_HOST: localhost
        DB_PASSWORD: postgres
    
    - name: Run integration tests
      run: |
        python -m pytest tests/integration/ -v
      env:
        LAKEFS_HOST: http://localhost:8000
        LAKEFS_ACCESS_KEY: AKIAIOSFODNN7EXAMPLE
        LAKEFS_SECRET_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
        DB_HOST: localhost
        DB_PASSWORD: postgres
    
    - name: Upload coverage reports
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
  
  build:
    needs: test
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Log in to Container Registry
      uses: docker/login-action@v2
      with:
        registry: ${{ env.REGISTRY }}
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}
    
    - name: Extract metadata
      id: meta
      uses: docker/metadata-action@v4
      with:
        images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
        tags: |
          type=ref,event=branch
          type=ref,event=pr
          type=sha
    
    - name: Build and push Docker image
      uses: docker/build-push-action@v4
      with:
        context: .
        file: deployment/docker/Dockerfile
        push: true
        tags: ${{ steps.meta.outputs.tags }}
        labels: ${{ steps.meta.outputs.labels }}
  
  deploy-staging:
    needs: build
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/develop'
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Deploy to staging
      run: |
        echo "Deploying to staging environment"
        # Add your staging deployment commands here
        # kubectl apply -f deployment/kubernetes/staging/
  
  deploy-production:
    needs: build
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    environment: production
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Deploy to production
      run: |
        echo "Deploying to production environment"
        # Add your production deployment commands here
        # kubectl apply -f deployment/kubernetes/production/
```

### Production Scripts

```python
# scripts/run_pipeline.py
#!/usr/bin/env python3
"""
Production pipeline runner script
"""

import os
import sys
import logging
import argparse
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.pipeline import MonitoredETLPipeline
from src.transformers.base_transformer import (
    DataCleaningTransformer,
    BusinessLogicTransformer,
    TransformationPipeline
)
from config.settings import config

def setup_logging():
    """Setup production logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('/app/logs/pipeline.log')
        ]
    )

def load_pipeline_config(config_file: str) -> dict:
    """Load pipeline configuration from file"""
    import yaml
    
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def create_transformation_pipeline(transform_config: dict) -> TransformationPipeline:
    """Create transformation pipeline from configuration"""
    
    transformers = []
    
    # Data cleaning transformer
    if 'cleaning_rules' in transform_config:
        transformers.append(DataCleaningTransformer(transform_config['cleaning_rules']))
    
    # Business logic transformer
    if 'business_rules' in transform_config:
        transformers.append(BusinessLogicTransformer(transform_config['business_rules']))
    
    return TransformationPipeline(transformers)

def main():
    parser = argparse.ArgumentParser(description='Run ETL Pipeline')
    parser.add_argument('--pipeline-name', required=True, help='Name of the pipeline to run')
    parser.add_argument('--config-file', required=True, help='Pipeline configuration file')
    parser.add_argument('--source-branch', default='main', help='Source branch for pipeline')
    parser.add_argument('--merge-to-main', action='store_true', help='Merge results to main branch')
    
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        pipeline_config = load_pipeline_config(args.config_file)
        
        # Create pipeline
        pipeline = MonitoredETLPipeline(
            pipeline_name=args.pipeline_name,
            alert_recipients=pipeline_config.get('alert_recipients', [])
        )
        
        # Setup validation rules
        validation_config = pipeline_config.get('validation', {})
        if 'schema' in validation_config:
            pipeline.validator.add_schema_validation(validation_config['schema'])
        if 'completeness_check' in validation_config:
            pipeline.validator.add_completeness_check(**validation_config['completeness_check'])
        if 'uniqueness_check' in validation_config:
            pipeline.validator.add_uniqueness_check(validation_config['uniqueness_check'])
        
        # Create transformation pipeline
        transformation_pipeline = create_transformation_pipeline(
            pipeline_config.get('transformations', {})
        )
        
        # Run pipeline
        logger.info(f"Starting pipeline: {args.pipeline_name}")
        
        run_result = pipeline.run_monitored_pipeline(
            extraction_config=pipeline_config['extraction'],
            transformation_pipeline=transformation_pipeline,
            load_config=pipeline_config['load'],
            source_branch=args.source_branch
        )
        
        logger.info(f"Pipeline completed: {run_result.status.value}")
        
        # Merge to main if requested and successful
        if args.merge_to_main and run_result.status.value == 'success':
            merge_commit = pipeline.merge_to_target('main')
            logger.info(f"Merged to main: {merge_commit}")
        
        # Exit with appropriate code
        sys.exit(0 if run_result.status.value == 'success' else 1)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    
    finally:
        if 'pipeline' in locals():
            pipeline.cleanup()

if __name__ == '__main__':
    main()
```

```python
# scripts/health_check.py
#!/usr/bin/env python3
"""
Health check script for ETL pipeline
"""

import sys
import os
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.settings import config
import lakefs

def check_lakefs_connection():
    """Check lakeFS connectivity"""
    try:
        client = lakefs.Client(
            host=config.lakefs.host,
            username=config.lakefs.username,
            password=config.lakefs.password
        )
        
        # Try to list repositories
        repos = client.repositories.list()
        return True, f"lakeFS connection OK, found {len(repos)} repositories"
        
    except Exception as e:
        return False, f"lakeFS connection failed: {str(e)}"

def check_database_connection():
    """Check database connectivity"""
    try:
        from src.extractors.database_extractor import DatabaseExtractor
        
        extractor = DatabaseExtractor()
        
        # Try a simple query
        with extractor.engine.connect() as conn:
            result = conn.execute("SELECT 1")
            result.fetchone()
        
        extractor.close()
        return True, "Database connection OK"
        
    except Exception as e:
        return False, f"Database connection failed: {str(e)}"

def main():
    """Run health checks"""
    
    logging.basicConfig(level=logging.WARNING)  # Reduce noise
    
    checks = [
        ("lakeFS", check_lakefs_connection),
        ("Database", check_database_connection)
    ]
    
    all_healthy = True
    
    for check_name, check_func in checks:
        try:
            healthy, message = check_func()
            status = "PASS" if healthy else "FAIL"
            print(f"{check_name}: {status} - {message}")
            
            if not healthy:
                all_healthy = False
                
        except Exception as e:
            print(f"{check_name}: FAIL - Unexpected error: {str(e)}")
            all_healthy = False
    
    if all_healthy:
        print("Overall health: HEALTHY")
        sys.exit(0)
    else:
        print("Overall health: UNHEALTHY")
        sys.exit(1)

if __name__ == '__main__':
    main()
```

## Best Practices and Production Considerations

### ETL Pipeline Best Practices

1. **Idempotency**
   - Ensure pipelines can be run multiple times safely
   - Use upsert operations instead of inserts where possible
   - Implement proper checkpointing

2. **Data Quality**
   - Validate data at every stage
   - Implement comprehensive error handling
   - Monitor data quality metrics

3. **Performance Optimization**
   - Process data in batches
   - Use appropriate data formats (Parquet for analytics)
   - Implement connection pooling

4. **Security**
   - Use environment variables for credentials
   - Implement proper access controls
   - Encrypt sensitive data

5. **Monitoring and Alerting**
   - Monitor pipeline execution metrics
   - Set up alerts for failures and SLA violations
   - Generate regular reports

### Troubleshooting Common Issues

1. **Memory Issues**
```python
# Process data in smaller chunks
def process_large_dataset(df, chunk_size=1000):
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        yield process_chunk(chunk)
```

2. **Connection Timeouts**
```python
# Implement retry logic with exponential backoff
@retry_on_failure(max_retries=3, base_delay=2.0)
def robust_database_operation():
    # Database operation here
    pass
```

3. **Data Validation Failures**
```python
# Implement graceful degradation
try:
    validate_data_strict(df)
except ValidationError:
    logger.warning("Strict validation failed, using lenient validation")
    validate_data_lenient(df)
```

## Next Steps

### Advanced Topics to Explore

1. **[ML Experiment Tracking](ml-experiment-tracking.md)** - Version models and experiments
2. **[Data Science Workflow](data-science-workflow.md)** - Interactive analysis patterns
3. **[Advanced Features](../high-level-sdk/advanced.md)** - Performance optimization
4. **[Best Practices](../reference/best-practices.md)** - Production deployment

### Integration Opportunities

1. **Apache Airflow** - Schedule and orchestrate ETL pipelines
2. **dbt** - Transform data with SQL-based transformations
3. **Great Expectations** - Advanced data validation and profiling
4. **Prometheus/Grafana** - Advanced monitoring and visualization

## See Also

**Prerequisites and Setup:**
- **[Python SDK Overview](../index.md)** - Compare all Python SDK options
- **[Getting Started Guide](../getting-started.md)** - Installation and authentication setup
- **[High-Level SDK Transactions](../high-level-sdk/transactions.md)** - Transaction patterns

**Related Tutorials:**
- **[Data Science Workflow](data-science-workflow.md)** - Interactive analysis patterns
- **[ML Experiment Tracking](ml-experiment-tracking.md)** - Model versioning workflows

**Advanced Features:**
- **[Error Handling](../reference/troubleshooting.md)** - Comprehensive error handling
- **[Best Practices](../reference/best-practices.md)** - Production deployment guidance
- **[API Comparison](../reference/api-comparison.md)** - SDK feature comparison

**External Resources:**
- **[Apache Airflow Documentation](https://airflow.apache.org/docs/){:target="_blank"}** - Workflow orchestration
- **[Great Expectations](https://greatexpectations.io/){:target="_blank"}** - Data validation framework
- **[Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/){:target="_blank"}** - Container deployment