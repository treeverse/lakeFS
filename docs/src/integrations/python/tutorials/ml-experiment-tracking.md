---
title: ML Experiment Tracking Tutorial
description: Build machine learning workflows with model versioning and experiment tracking using lakeFS
sdk_types: ["high-level", "lakefs-spec"]
difficulty: "advanced"
use_cases: ["ml", "experiment-tracking", "model-versioning", "data-science"]
topics: ["machine-learning", "experiments", "models", "versioning", "mlops"]
audience: ["data-scientists", "ml-engineers", "researchers"]
last_updated: "2024-01-15"
---

# ML Experiment Tracking Tutorial

Learn how to build comprehensive machine learning workflows with experiment tracking, model versioning, and dataset management using lakeFS. This tutorial demonstrates MLOps best practices for reproducible machine learning experiments, model lifecycle management, and collaborative ML development.

## What You'll Build

By the end of this tutorial, you'll have:

- **Experiment Management System** - Track ML experiments with full reproducibility
- **Model Versioning Pipeline** - Version models with metadata and lineage
- **Dataset Management** - Version datasets and track data lineage
- **Model Registry** - Centralized model storage and deployment tracking
- **A/B Testing Framework** - Compare model performance across experiments
- **MLOps Pipeline** - Production-ready ML deployment workflow

## Prerequisites

### Knowledge Requirements
- Intermediate Python and machine learning concepts
- Familiarity with scikit-learn, pandas, and numpy
- Understanding of ML model lifecycle
- Basic knowledge of model evaluation metrics

### Environment Setup
- Python 3.8+ installed
- Jupyter notebook environment
- lakeFS server running (local or cloud)
- Required ML libraries (we'll install these)

### lakeFS Setup
```bash
# Start lakeFS locally (if not already running)
docker run --name lakefs --rm -p 8000:8000 treeverse/lakefs:latest run --local-settings
```

## Step 1: Environment Setup and ML Dependencies

### Install Required Packages

```bash
# Install lakeFS and ML libraries
pip install lakefs lakefs-spec pandas numpy
pip install scikit-learn xgboost lightgbm
pip install matplotlib seaborn plotly
pip install mlflow optuna hyperopt
pip install joblib pickle-mixin
pip install jupyter ipywidgets
```

### Project Structure Setup

```bash
# Create ML project structure
mkdir lakefs-ml-experiments
cd lakefs-ml-experiments

# Create directory structure
mkdir -p {data,models,experiments,notebooks,scripts,configs,reports}

# Create configuration files
touch configs/{model_config.yaml,experiment_config.yaml}
```

### ML Experiment Configuration

```python
# configs/ml_config.py
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import yaml

@dataclass
class LakeFSConfig:
    host: str
    username: str
    password: str
    repository: str

@dataclass
class ExperimentConfig:
    experiment_name: str
    description: str
    tags: List[str]
    parameters: Dict[str, Any]
    metrics_to_track: List[str]
    artifacts_to_save: List[str]

@dataclass
class ModelConfig:
    model_type: str
    hyperparameters: Dict[str, Any]
    feature_columns: List[str]
    target_column: str
    validation_split: float
    random_state: int

class MLConfigManager:
    """Configuration management for ML experiments"""
    
    def __init__(self, config_path: str = "configs"):
        self.config_path = config_path
        self._load_configs()
    
    def _load_configs(self):
        """Load all configuration files"""
        # lakeFS configuration
        self.lakefs = LakeFSConfig(
            host=os.getenv('LAKEFS_HOST', 'http://localhost:8000'),
            username=os.getenv('LAKEFS_ACCESS_KEY', 'AKIAIOSFODNN7EXAMPLE'),
            password=os.getenv('LAKEFS_SECRET_KEY', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'),
            repository=os.getenv('LAKEFS_REPOSITORY', 'ml-experiments')
        )
    
    def create_experiment_config(
        self,
        experiment_name: str,
        description: str,
        model_type: str,
        hyperparameters: Dict[str, Any],
        tags: List[str] = None
    ) -> ExperimentConfig:
        """Create experiment configuration"""
        
        return ExperimentConfig(
            experiment_name=experiment_name,
            description=description,
            tags=tags or [],
            parameters=hyperparameters,
            metrics_to_track=['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc'],
            artifacts_to_save=['model', 'feature_importance', 'confusion_matrix', 'roc_curve']
        )
    
    def create_model_config(
        self,
        model_type: str,
        hyperparameters: Dict[str, Any],
        feature_columns: List[str],
        target_column: str
    ) -> ModelConfig:
        """Create model configuration"""
        
        return ModelConfig(
            model_type=model_type,
            hyperparameters=hyperparameters,
            feature_columns=feature_columns,
            target_column=target_column,
            validation_split=0.2,
            random_state=42
        )

# Global configuration instance
ml_config = MLConfigManager()
```

## Step 2: ML Repository Setup and Data Management

### Create ML Repository

```python
# scripts/setup_ml_repository.py
import lakefs
from lakefs.exceptions import RepositoryExistsError
import pandas as pd
import numpy as np
from sklearn.datasets import make_classification, make_regression
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

def setup_ml_repository():
    """Initialize lakeFS repository for ML experiments"""
    
    client = lakefs.Client(
        host='http://localhost:8000',
        username='AKIAIOSFODNN7EXAMPLE',
        password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    )
    
    # Create repository
    try:
        repo = client.repositories.create(
            name='ml-experiments',
            storage_namespace='s3://my-bucket/ml-experiments/',
            default_branch='main'
        )
        logger.info(f"Created repository: {repo.id}")
    except RepositoryExistsError:
        repo = client.repositories.get('ml-experiments')
        logger.info(f"Using existing repository: {repo.id}")
    
    # Create standard branches for ML workflow
    branches_to_create = [
        ('development', 'main'),
        ('staging', 'main'),
        ('production', 'main'),
        ('experiments', 'main')
    ]
    
    for branch_name, source in branches_to_create:
        try:
            branch = repo.branches.create(branch_name, source_reference=source)
            logger.info(f"Created branch: {branch_name}")
        except Exception as e:
            logger.info(f"Branch {branch_name} already exists or error: {e}")
    
    return repo

def create_ml_directory_structure(repo):
    """Create ML-specific directory structure in lakeFS"""
    
    main_branch = repo.branches.get('main')
    
    # Create directory structure with placeholder files
    directories = [
        'datasets/raw/',
        'datasets/processed/',
        'datasets/features/',
        'models/trained/',
        'models/registered/',
        'models/deployed/',
        'experiments/runs/',
        'experiments/results/',
        'experiments/artifacts/',
        'notebooks/',
        'reports/',
        'configs/',
        'logs/'
    ]
    
    for directory in directories:
        try:
            main_branch.objects.upload(
                path=f"{directory}.gitkeep",
                data=b"# ML directory placeholder",
                content_type='text/plain'
            )
        except Exception as e:
            logger.warning(f"Could not create {directory}: {e}")
    
    # Commit directory structure
    commit = main_branch.commits.create(
        message="Initialize ML experiment directory structure",
        metadata={'setup': 'ml_directory_structure', 'type': 'ml_setup'}
    )
    
    logger.info(f"ML directory structure created: {commit.id}")
    return commit

def generate_sample_datasets(repo):
    """Generate sample datasets for ML experiments"""
    
    main_branch = repo.branches.get('main')
    
    # Generate classification dataset
    X_class, y_class = make_classification(
        n_samples=10000,
        n_features=20,
        n_informative=15,
        n_redundant=5,
        n_classes=2,
        random_state=42
    )
    
    # Create feature names
    feature_names = [f'feature_{i}' for i in range(X_class.shape[1])]
    
    # Create classification DataFrame
    classification_df = pd.DataFrame(X_class, columns=feature_names)
    classification_df['target'] = y_class
    classification_df['customer_id'] = range(len(classification_df))
    classification_df['timestamp'] = pd.date_range('2023-01-01', periods=len(classification_df), freq='H')
    
    # Add some realistic business context
    classification_df['age'] = np.random.randint(18, 80, len(classification_df))
    classification_df['income'] = np.random.exponential(50000, len(classification_df))
    classification_df['region'] = np.random.choice(['North', 'South', 'East', 'West'], len(classification_df))
    
    # Save classification dataset
    classification_csv = classification_df.to_csv(index=False)
    main_branch.objects.upload(
        path='datasets/raw/customer_churn_dataset.csv',
        data=classification_csv.encode(),
        content_type='text/csv'
    )
    
    # Generate regression dataset
    X_reg, y_reg = make_regression(
        n_samples=8000,
        n_features=15,
        n_informative=10,
        noise=0.1,
        random_state=42
    )
    
    # Create regression DataFrame
    regression_feature_names = [f'feature_{i}' for i in range(X_reg.shape[1])]
    regression_df = pd.DataFrame(X_reg, columns=regression_feature_names)
    regression_df['target'] = y_reg
    regression_df['property_id'] = range(len(regression_df))
    regression_df['timestamp'] = pd.date_range('2023-01-01', periods=len(regression_df), freq='2H')
    
    # Add realistic property features
    regression_df['square_feet'] = np.random.randint(500, 5000, len(regression_df))
    regression_df['bedrooms'] = np.random.randint(1, 6, len(regression_df))
    regression_df['bathrooms'] = np.random.randint(1, 4, len(regression_df))
    regression_df['location_score'] = np.random.uniform(1, 10, len(regression_df))
    
    # Save regression dataset
    regression_csv = regression_df.to_csv(index=False)
    main_branch.objects.upload(
        path='datasets/raw/house_price_dataset.csv',
        data=regression_csv.encode(),
        content_type='text/csv'
    )
    
    # Create dataset metadata
    datasets_metadata = {
        'customer_churn_dataset': {
            'type': 'classification',
            'target_column': 'target',
            'n_samples': len(classification_df),
            'n_features': len(feature_names),
            'classes': [0, 1],
            'description': 'Customer churn prediction dataset',
            'created_at': datetime.now().isoformat()
        },
        'house_price_dataset': {
            'type': 'regression',
            'target_column': 'target',
            'n_samples': len(regression_df),
            'n_features': len(regression_feature_names),
            'description': 'House price prediction dataset',
            'created_at': datetime.now().isoformat()
        }
    }
    
    main_branch.objects.upload(
        path='datasets/metadata.json',
        data=json.dumps(datasets_metadata, indent=2).encode(),
        content_type='application/json'
    )
    
    # Commit datasets
    commit = main_branch.commits.create(
        message="Add sample ML datasets: customer churn and house prices",
        metadata={
            'datasets_added': 2,
            'classification_samples': len(classification_df),
            'regression_samples': len(regression_df)
        }
    )
    
    logger.info(f"Sample datasets created: {commit.id}")
    return commit

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    repo = setup_ml_repository()
    create_ml_directory_structure(repo)
    generate_sample_datasets(repo)
    
    print("ML repository setup complete!")
```

## Step 3: Experiment Tracking Framework

### Core Experiment Tracking System

```python
# src/experiment_tracker.py
import lakefs
import lakefs_spec
import pandas as pd
import numpy as np
import json
import pickle
import joblib
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import logging
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, mean_squared_error, mean_absolute_error, r2_score,
    confusion_matrix, roc_curve, precision_recall_curve
)
import io

logger = logging.getLogger(__name__)

@dataclass
class ExperimentRun:
    """Represents a single ML experiment run"""
    run_id: str
    experiment_name: str
    model_type: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"  # running, completed, failed
    parameters: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    artifacts: Dict[str, str] = field(default_factory=dict)  # artifact_name -> path
    dataset_version: Optional[str] = None
    model_version: Optional[str] = None
    notes: str = ""
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'run_id': self.run_id,
            'experiment_name': self.experiment_name,
            'model_type': self.model_type,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status,
            'parameters': self.parameters,
            'metrics': self.metrics,
            'artifacts': self.artifacts,
            'dataset_version': self.dataset_version,
            'model_version': self.model_version,
            'notes': self.notes,
            'tags': self.tags
        }

class MLExperimentTracker:
    """Comprehensive ML experiment tracking with lakeFS"""
    
    def __init__(self, repository_name: str = 'ml-experiments'):
        self.client = lakefs.Client(
            host='http://localhost:8000',
            username='AKIAIOSFODNN7EXAMPLE',
            password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        )
        self.repo = self.client.repositories.get(repository_name)
        self.fs = lakefs_spec.LakeFSFileSystem(
            host='http://localhost:8000',
            username='AKIAIOSFODNN7EXAMPLE',
            password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        )
        self.logger = logging.getLogger(__name__)
        
        # Current experiment state
        self.current_run: Optional[ExperimentRun] = None
        self.current_branch: Optional[str] = None
    
    def start_experiment(
        self,
        experiment_name: str,
        model_type: str,
        parameters: Dict[str, Any],
        dataset_version: Optional[str] = None,
        notes: str = "",
        tags: List[str] = None,
        branch_name: Optional[str] = None
    ) -> ExperimentRun:
        """Start a new experiment run"""
        
        run_id = str(uuid.uuid4())
        
        # Create experiment branch if not provided
        if branch_name is None:
            branch_name = f"experiment/{experiment_name}/{run_id[:8]}"
        
        try:
            # Create experiment branch
            experiment_branch = self.repo.branches.create(
                branch_name, 
                source_reference='experiments'
            )
            self.current_branch = branch_name
            self.logger.info(f"Created experiment branch: {branch_name}")
        except Exception as e:
            # Branch might already exist
            experiment_branch = self.repo.branches.get(branch_name)
            self.current_branch = branch_name
            self.logger.info(f"Using existing experiment branch: {branch_name}")
        
        # Initialize experiment run
        self.current_run = ExperimentRun(
            run_id=run_id,
            experiment_name=experiment_name,
            model_type=model_type,
            start_time=datetime.now(),
            parameters=parameters,
            dataset_version=dataset_version,
            notes=notes,
            tags=tags or []
        )
        
        # Save initial experiment metadata
        self._save_experiment_metadata(experiment_branch)
        
        self.logger.info(f"Started experiment: {experiment_name} (run_id: {run_id})")
        return self.current_run
    
    def log_parameter(self, key: str, value: Any):
        """Log a parameter for the current experiment"""
        if self.current_run:
            self.current_run.parameters[key] = value
            self.logger.debug(f"Logged parameter: {key} = {value}")
    
    def log_metric(self, key: str, value: float):
        """Log a metric for the current experiment"""
        if self.current_run:
            self.current_run.metrics[key] = value
            self.logger.debug(f"Logged metric: {key} = {value}")
    
    def log_metrics(self, metrics: Dict[str, float]):
        """Log multiple metrics at once"""
        for key, value in metrics.items():
            self.log_metric(key, value)
    
    def log_artifact(self, artifact_name: str, artifact_path: str, artifact_data: Any = None):
        """Log an artifact (model, plot, etc.) for the current experiment"""
        if not self.current_run or not self.current_branch:
            raise ValueError("No active experiment run")
        
        branch = self.repo.branches.get(self.current_branch)
        
        # Determine storage path
        storage_path = f"experiments/runs/{self.current_run.run_id}/artifacts/{artifact_name}"
        
        # Save artifact data if provided
        if artifact_data is not None:
            if isinstance(artifact_data, plt.Figure):
                # Save matplotlib figure
                buffer = io.BytesIO()
                artifact_data.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
                buffer.seek(0)
                
                branch.objects.upload(
                    path=f"{storage_path}.png",
                    data=buffer.getvalue(),
                    content_type='image/png'
                )
                storage_path = f"{storage_path}.png"
                
            elif hasattr(artifact_data, 'save'):
                # Save scikit-learn model or similar
                buffer = io.BytesIO()
                joblib.dump(artifact_data, buffer)
                
                branch.objects.upload(
                    path=f"{storage_path}.joblib",
                    data=buffer.getvalue(),
                    content_type='application/octet-stream'
                )
                storage_path = f"{storage_path}.joblib"
                
            elif isinstance(artifact_data, (dict, list)):
                # Save JSON data
                json_data = json.dumps(artifact_data, indent=2, default=str)
                
                branch.objects.upload(
                    path=f"{storage_path}.json",
                    data=json_data.encode(),
                    content_type='application/json'
                )
                storage_path = f"{storage_path}.json"
                
            elif isinstance(artifact_data, pd.DataFrame):
                # Save DataFrame as CSV
                csv_data = artifact_data.to_csv(index=False)
                
                branch.objects.upload(
                    path=f"{storage_path}.csv",
                    data=csv_data.encode(),
                    content_type='text/csv'
                )
                storage_path = f"{storage_path}.csv"
        
        # Record artifact path
        self.current_run.artifacts[artifact_name] = storage_path
        self.logger.info(f"Logged artifact: {artifact_name} -> {storage_path}")
    
    def log_model(self, model, model_name: str = "model", metadata: Dict[str, Any] = None):
        """Log a trained model with metadata"""
        if not self.current_run or not self.current_branch:
            raise ValueError("No active experiment run")
        
        branch = self.repo.branches.get(self.current_branch)
        
        # Save model
        model_path = f"experiments/runs/{self.current_run.run_id}/models/{model_name}.joblib"
        
        buffer = io.BytesIO()
        joblib.dump(model, buffer)
        
        branch.objects.upload(
            path=model_path,
            data=buffer.getvalue(),
            content_type='application/octet-stream'
        )
        
        # Save model metadata
        model_metadata = {
            'model_name': model_name,
            'model_type': self.current_run.model_type,
            'run_id': self.current_run.run_id,
            'experiment_name': self.current_run.experiment_name,
            'created_at': datetime.now().isoformat(),
            'parameters': self.current_run.parameters,
            'metrics': self.current_run.metrics,
            'custom_metadata': metadata or {}
        }
        
        metadata_path = f"experiments/runs/{self.current_run.run_id}/models/{model_name}_metadata.json"
        branch.objects.upload(
            path=metadata_path,
            data=json.dumps(model_metadata, indent=2).encode(),
            content_type='application/json'
        )
        
        # Update run artifacts
        self.current_run.artifacts[f"{model_name}_model"] = model_path
        self.current_run.artifacts[f"{model_name}_metadata"] = metadata_path
        self.current_run.model_version = model_path
        
        self.logger.info(f"Logged model: {model_name} -> {model_path}")
    
    def evaluate_model(self, model, X_test, y_test, task_type: str = 'classification') -> Dict[str, float]:
        """Evaluate model and log metrics automatically"""
        
        y_pred = model.predict(X_test)
        metrics = {}
        
        if task_type == 'classification':
            # Classification metrics
            metrics['accuracy'] = accuracy_score(y_test, y_pred)
            metrics['precision'] = precision_score(y_test, y_pred, average='weighted')
            metrics['recall'] = recall_score(y_test, y_pred, average='weighted')
            metrics['f1_score'] = f1_score(y_test, y_pred, average='weighted')
            
            # ROC AUC for binary classification
            if len(np.unique(y_test)) == 2:
                if hasattr(model, 'predict_proba'):
                    y_pred_proba = model.predict_proba(X_test)[:, 1]
                    metrics['roc_auc'] = roc_auc_score(y_test, y_pred_proba)
                elif hasattr(model, 'decision_function'):
                    y_pred_scores = model.decision_function(X_test)
                    metrics['roc_auc'] = roc_auc_score(y_test, y_pred_scores)
            
            # Log confusion matrix
            cm = confusion_matrix(y_test, y_pred)
            self.log_artifact('confusion_matrix', 'confusion_matrix', cm.tolist())
            
            # Create and log confusion matrix plot
            plt.figure(figsize=(8, 6))
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
            plt.title('Confusion Matrix')
            plt.ylabel('True Label')
            plt.xlabel('Predicted Label')
            self.log_artifact('confusion_matrix_plot', 'confusion_matrix_plot', plt.gcf())
            plt.close()
            
        elif task_type == 'regression':
            # Regression metrics
            metrics['mse'] = mean_squared_error(y_test, y_pred)
            metrics['rmse'] = np.sqrt(metrics['mse'])
            metrics['mae'] = mean_absolute_error(y_test, y_pred)
            metrics['r2_score'] = r2_score(y_test, y_pred)
            
            # Create and log residuals plot
            residuals = y_test - y_pred
            plt.figure(figsize=(10, 6))
            
            plt.subplot(1, 2, 1)
            plt.scatter(y_pred, residuals, alpha=0.6)
            plt.axhline(y=0, color='r', linestyle='--')
            plt.xlabel('Predicted Values')
            plt.ylabel('Residuals')
            plt.title('Residuals vs Predicted')
            
            plt.subplot(1, 2, 2)
            plt.hist(residuals, bins=30, alpha=0.7)
            plt.xlabel('Residuals')
            plt.ylabel('Frequency')
            plt.title('Residuals Distribution')
            
            plt.tight_layout()
            self.log_artifact('residuals_plot', 'residuals_plot', plt.gcf())
            plt.close()
        
        # Log all metrics
        self.log_metrics(metrics)
        
        return metrics
    
    def end_experiment(self, status: str = "completed", notes: str = ""):
        """End the current experiment run"""
        if not self.current_run or not self.current_branch:
            raise ValueError("No active experiment run")
        
        self.current_run.end_time = datetime.now()
        self.current_run.status = status
        if notes:
            self.current_run.notes += f"\n{notes}"
        
        # Save final experiment metadata
        branch = self.repo.branches.get(self.current_branch)
        self._save_experiment_metadata(branch)
        
        # Commit experiment results
        duration = (self.current_run.end_time - self.current_run.start_time).total_seconds()
        
        commit = branch.commits.create(
            message=f"Complete experiment: {self.current_run.experiment_name}",
            metadata={
                'experiment_name': self.current_run.experiment_name,
                'run_id': self.current_run.run_id,
                'status': status,
                'duration_seconds': str(duration),
                'metrics': json.dumps(self.current_run.metrics)
            }
        )
        
        self.logger.info(f"Experiment completed: {self.current_run.experiment_name} (duration: {duration:.2f}s)")
        
        # Reset current state
        completed_run = self.current_run
        self.current_run = None
        self.current_branch = None
        
        return completed_run, commit.id
    
    def _save_experiment_metadata(self, branch):
        """Save experiment metadata to lakeFS"""
        if self.current_run:
            metadata_path = f"experiments/runs/{self.current_run.run_id}/metadata.json"
            metadata_json = json.dumps(self.current_run.to_dict(), indent=2)
            
            branch.objects.upload(
                path=metadata_path,
                data=metadata_json.encode(),
                content_type='application/json'
            )
    
    def list_experiments(self, experiment_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all experiments or experiments by name"""
        
        experiments = []
        
        try:
            # List all experiment run directories
            experiments_branch = self.repo.branches.get('experiments')
            
            # Get all run directories
            run_objects = experiments_branch.objects.list(prefix='experiments/runs/')
            
            run_ids = set()
            for obj in run_objects:
                path_parts = obj.path.split('/')
                if len(path_parts) >= 3 and path_parts[2] not in run_ids:
                    run_ids.add(path_parts[2])
            
            # Load metadata for each run
            for run_id in run_ids:
                try:
                    metadata_obj = experiments_branch.objects.get(f'experiments/runs/{run_id}/metadata.json')
                    metadata = json.loads(metadata_obj.reader().read().decode())
                    
                    if experiment_name is None or metadata.get('experiment_name') == experiment_name:
                        experiments.append(metadata)
                        
                except Exception as e:
                    self.logger.warning(f"Could not load metadata for run {run_id}: {e}")
            
            # Sort by start time
            experiments.sort(key=lambda x: x.get('start_time', ''), reverse=True)
            
        except Exception as e:
            self.logger.error(f"Error listing experiments: {e}")
        
        return experiments
    
    def load_model(self, run_id: str, model_name: str = "model"):
        """Load a model from a specific experiment run"""
        
        try:
            experiments_branch = self.repo.branches.get('experiments')
            model_path = f"experiments/runs/{run_id}/models/{model_name}.joblib"
            
            model_obj = experiments_branch.objects.get(model_path)
            model_data = model_obj.reader().read()
            
            # Load model from bytes
            model = joblib.load(io.BytesIO(model_data))
            
            self.logger.info(f"Loaded model: {model_name} from run {run_id}")
            return model
            
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            raise
    
    def compare_experiments(self, run_ids: List[str]) -> pd.DataFrame:
        """Compare multiple experiment runs"""
        
        comparison_data = []
        
        for run_id in run_ids:
            try:
                experiments_branch = self.repo.branches.get('experiments')
                metadata_obj = experiments_branch.objects.get(f'experiments/runs/{run_id}/metadata.json')
                metadata = json.loads(metadata_obj.reader().read().decode())
                
                # Flatten data for comparison
                row = {
                    'run_id': run_id,
                    'experiment_name': metadata.get('experiment_name'),
                    'model_type': metadata.get('model_type'),
                    'status': metadata.get('status'),
                    'start_time': metadata.get('start_time')
                }
                
                # Add parameters
                for key, value in metadata.get('parameters', {}).items():
                    row[f'param_{key}'] = value
                
                # Add metrics
                for key, value in metadata.get('metrics', {}).items():
                    row[f'metric_{key}'] = value
                
                comparison_data.append(row)
                
            except Exception as e:
                self.logger.warning(f"Could not load data for run {run_id}: {e}")
        
        return pd.DataFrame(comparison_data)

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create experiment tracker
    tracker = MLExperimentTracker()
    
    # Start an experiment
    run = tracker.start_experiment(
        experiment_name="customer_churn_prediction",
        model_type="random_forest",
        parameters={
            'n_estimators': 100,
            'max_depth': 10,
            'random_state': 42
        },
        notes="Initial baseline model",
        tags=['baseline', 'random_forest']
    )
    
    print(f"Started experiment: {run.run_id}")
    
    # Log additional parameters and metrics
    tracker.log_parameter('feature_selection', 'all')
    tracker.log_metric('training_time', 45.2)
    
    # End experiment
    completed_run, commit_id = tracker.end_experiment("completed")
    print(f"Experiment completed: {commit_id}")
```#
# Step 4: Complete ML Workflow Implementation

### End-to-End ML Pipeline

```python
# src/ml_pipeline.py
import lakefs
import lakefs_spec
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.svm import SVC, SVR
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, regression_report
import xgboost as xgb
import lightgbm as lgb
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional, Tuple
import json

from src.experiment_tracker import MLExperimentTracker

logger = logging.getLogger(__name__)

class MLPipeline:
    """Complete ML pipeline with experiment tracking"""
    
    def __init__(self, repository_name: str = 'ml-experiments'):
        self.tracker = MLExperimentTracker(repository_name)
        self.repo = self.tracker.repo
        self.fs = self.tracker.fs
        self.logger = logging.getLogger(__name__)
        
        # Available models
        self.models = {
            'random_forest_classifier': RandomForestClassifier,
            'random_forest_regressor': RandomForestRegressor,
            'logistic_regression': LogisticRegression,
            'linear_regression': LinearRegression,
            'svc': SVC,
            'svr': SVR,
            'xgboost_classifier': xgb.XGBClassifier,
            'xgboost_regressor': xgb.XGBRegressor,
            'lightgbm_classifier': lgb.LGBMClassifier,
            'lightgbm_regressor': lgb.LGBMRegressor
        }
    
    def load_dataset(self, dataset_path: str, branch: str = 'main') -> pd.DataFrame:
        """Load dataset from lakeFS"""
        
        full_path = f"lakefs://ml-experiments/{branch}/{dataset_path}"
        
        try:
            if dataset_path.endswith('.csv'):
                df = pd.read_csv(full_path, filesystem=self.fs)
            elif dataset_path.endswith('.parquet'):
                df = pd.read_parquet(full_path, filesystem=self.fs)
            else:
                raise ValueError(f"Unsupported file format: {dataset_path}")
            
            self.logger.info(f"Loaded dataset: {dataset_path} ({len(df)} rows, {len(df.columns)} columns)")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading dataset {dataset_path}: {e}")
            raise
    
    def preprocess_data(
        self,
        df: pd.DataFrame,
        target_column: str,
        feature_columns: Optional[List[str]] = None,
        test_size: float = 0.2,
        random_state: int = 42
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """Preprocess data for ML training"""
        
        # Select features
        if feature_columns is None:
            feature_columns = [col for col in df.columns if col != target_column]
        
        X = df[feature_columns].copy()
        y = df[target_column].copy()
        
        # Handle missing values
        X = X.fillna(X.mean() if X.select_dtypes(include=[np.number]).shape[1] > 0 else X.mode().iloc[0])
        
        # Encode categorical variables
        categorical_columns = X.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            le = LabelEncoder()
            X[col] = le.fit_transform(X[col].astype(str))
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y if len(np.unique(y)) < 20 else None
        )
        
        self.logger.info(f"Data preprocessing complete: {len(X_train)} train, {len(X_test)} test samples")
        
        return X_train, X_test, y_train, y_test
    
    def run_experiment(
        self,
        experiment_name: str,
        dataset_path: str,
        target_column: str,
        model_type: str,
        hyperparameters: Dict[str, Any],
        feature_columns: Optional[List[str]] = None,
        task_type: str = 'classification',
        cross_validation: bool = True,
        hyperparameter_tuning: bool = False,
        tuning_params: Optional[Dict[str, List]] = None,
        notes: str = "",
        tags: List[str] = None
    ) -> Tuple[Any, Dict[str, float]]:
        """Run a complete ML experiment"""
        
        # Start experiment tracking
        run = self.tracker.start_experiment(
            experiment_name=experiment_name,
            model_type=model_type,
            parameters=hyperparameters,
            notes=notes,
            tags=tags or []
        )
        
        try:
            # Load and preprocess data
            df = self.load_dataset(dataset_path)
            X_train, X_test, y_train, y_test = self.preprocess_data(
                df, target_column, feature_columns
            )
            
            # Log dataset information
            self.tracker.log_parameter('dataset_path', dataset_path)
            self.tracker.log_parameter('target_column', target_column)
            self.tracker.log_parameter('n_features', len(X_train.columns))
            self.tracker.log_parameter('n_train_samples', len(X_train))
            self.tracker.log_parameter('n_test_samples', len(X_test))
            self.tracker.log_parameter('task_type', task_type)
            
            # Create model
            if model_type not in self.models:
                raise ValueError(f"Unknown model type: {model_type}")
            
            model_class = self.models[model_type]
            
            # Hyperparameter tuning
            if hyperparameter_tuning and tuning_params:
                self.logger.info("Starting hyperparameter tuning...")
                
                # Create base model
                base_model = model_class(**hyperparameters)
                
                # Grid search
                grid_search = GridSearchCV(
                    base_model,
                    tuning_params,
                    cv=5,
                    scoring='accuracy' if task_type == 'classification' else 'r2',
                    n_jobs=-1,
                    verbose=1
                )
                
                grid_search.fit(X_train, y_train)
                
                # Use best parameters
                best_params = grid_search.best_params_
                hyperparameters.update(best_params)
                
                # Log tuning results
                self.tracker.log_parameter('hyperparameter_tuning', True)
                self.tracker.log_parameter('best_params', best_params)
                self.tracker.log_metric('best_cv_score', grid_search.best_score_)
                
                model = grid_search.best_estimator_
                
            else:
                # Create model with given hyperparameters
                model = model_class(**hyperparameters)
                model.fit(X_train, y_train)
            
            # Cross-validation
            if cross_validation:
                cv_scores = cross_val_score(
                    model, X_train, y_train, cv=5,
                    scoring='accuracy' if task_type == 'classification' else 'r2'
                )
                
                self.tracker.log_metric('cv_mean', cv_scores.mean())
                self.tracker.log_metric('cv_std', cv_scores.std())
                self.tracker.log_artifact('cv_scores', 'cv_scores', cv_scores.tolist())
            
            # Evaluate model
            metrics = self.tracker.evaluate_model(model, X_test, y_test, task_type)
            
            # Feature importance (if available)
            if hasattr(model, 'feature_importances_'):
                feature_importance = pd.DataFrame({
                    'feature': X_train.columns,
                    'importance': model.feature_importances_
                }).sort_values('importance', ascending=False)
                
                self.tracker.log_artifact('feature_importance', 'feature_importance', feature_importance)
                
                # Create feature importance plot
                import matplotlib.pyplot as plt
                plt.figure(figsize=(10, 8))
                top_features = feature_importance.head(20)
                plt.barh(range(len(top_features)), top_features['importance'])
                plt.yticks(range(len(top_features)), top_features['feature'])
                plt.xlabel('Feature Importance')
                plt.title('Top 20 Feature Importances')
                plt.gca().invert_yaxis()
                plt.tight_layout()
                
                self.tracker.log_artifact('feature_importance_plot', 'feature_importance_plot', plt.gcf())
                plt.close()
            
            # Log model
            self.tracker.log_model(model, 'trained_model', {
                'feature_columns': list(X_train.columns),
                'target_column': target_column,
                'model_class': model_class.__name__
            })
            
            # Log training data sample
            train_sample = X_train.head(100).copy()
            train_sample[target_column] = y_train.head(100)
            self.tracker.log_artifact('training_data_sample', 'training_data_sample', train_sample)
            
            # End experiment
            completed_run, commit_id = self.tracker.end_experiment("completed")
            
            self.logger.info(f"Experiment completed successfully: {experiment_name}")
            
            return model, metrics
            
        except Exception as e:
            self.logger.error(f"Experiment failed: {e}")
            self.tracker.end_experiment("failed", f"Error: {str(e)}")
            raise
    
    def run_hyperparameter_optimization(
        self,
        experiment_name: str,
        dataset_path: str,
        target_column: str,
        model_type: str,
        param_space: Dict[str, List],
        n_trials: int = 50,
        task_type: str = 'classification'
    ):
        """Run hyperparameter optimization using Optuna"""
        
        try:
            import optuna
        except ImportError:
            raise ImportError("Optuna is required for hyperparameter optimization. Install with: pip install optuna")
        
        # Load data
        df = self.load_dataset(dataset_path)
        X_train, X_test, y_train, y_test = self.preprocess_data(df, target_column)
        
        def objective(trial):
            # Sample hyperparameters
            params = {}
            for param_name, param_values in param_space.items():
                if isinstance(param_values[0], int):
                    params[param_name] = trial.suggest_int(param_name, min(param_values), max(param_values))
                elif isinstance(param_values[0], float):
                    params[param_name] = trial.suggest_float(param_name, min(param_values), max(param_values))
                else:
                    params[param_name] = trial.suggest_categorical(param_name, param_values)
            
            # Train model
            model_class = self.models[model_type]
            model = model_class(**params)
            
            # Cross-validation score
            cv_scores = cross_val_score(
                model, X_train, y_train, cv=3,
                scoring='accuracy' if task_type == 'classification' else 'r2'
            )
            
            return cv_scores.mean()
        
        # Run optimization
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials)
        
        # Run experiment with best parameters
        best_params = study.best_params
        
        model, metrics = self.run_experiment(
            experiment_name=f"{experiment_name}_optimized",
            dataset_path=dataset_path,
            target_column=target_column,
            model_type=model_type,
            hyperparameters=best_params,
            task_type=task_type,
            notes=f"Hyperparameter optimization with {n_trials} trials. Best score: {study.best_value:.4f}",
            tags=['optimized', 'optuna']
        )
        
        return model, metrics, study
    
    def compare_models(
        self,
        experiment_name: str,
        dataset_path: str,
        target_column: str,
        models_to_compare: List[Dict[str, Any]],
        task_type: str = 'classification'
    ) -> pd.DataFrame:
        """Compare multiple models on the same dataset"""
        
        results = []
        
        for i, model_config in enumerate(models_to_compare):
            model_type = model_config['model_type']
            hyperparameters = model_config.get('hyperparameters', {})
            
            try:
                model, metrics = self.run_experiment(
                    experiment_name=f"{experiment_name}_comparison_{i+1}",
                    dataset_path=dataset_path,
                    target_column=target_column,
                    model_type=model_type,
                    hyperparameters=hyperparameters,
                    task_type=task_type,
                    notes=f"Model comparison experiment {i+1}/{len(models_to_compare)}",
                    tags=['comparison', model_type]
                )
                
                result = {
                    'model_type': model_type,
                    'experiment_name': f"{experiment_name}_comparison_{i+1}",
                    **hyperparameters,
                    **metrics
                }
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Failed to train {model_type}: {e}")
                result = {
                    'model_type': model_type,
                    'experiment_name': f"{experiment_name}_comparison_{i+1}",
                    'error': str(e)
                }
                results.append(result)
        
        comparison_df = pd.DataFrame(results)
        
        # Save comparison results
        experiments_branch = self.repo.branches.get('experiments')
        comparison_path = f"experiments/comparisons/{experiment_name}_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        comparison_csv = comparison_df.to_csv(index=False)
        experiments_branch.objects.upload(
            path=comparison_path,
            data=comparison_csv.encode(),
            content_type='text/csv'
        )
        
        self.logger.info(f"Model comparison complete. Results saved to {comparison_path}")
        
        return comparison_df

# Example usage and complete workflow
def run_complete_ml_workflow():
    """Demonstrate complete ML workflow with experiment tracking"""
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize ML pipeline
    ml_pipeline = MLPipeline()
    
    # 1. Classification experiment
    print("=== Running Classification Experiments ===")
    
    # Baseline Random Forest
    rf_model, rf_metrics = ml_pipeline.run_experiment(
        experiment_name="customer_churn_baseline",
        dataset_path="datasets/raw/customer_churn_dataset.csv",
        target_column="target",
        model_type="random_forest_classifier",
        hyperparameters={
            'n_estimators': 100,
            'max_depth': 10,
            'random_state': 42
        },
        task_type='classification',
        notes="Baseline Random Forest model for customer churn prediction",
        tags=['baseline', 'random_forest', 'churn']
    )
    
    print(f"Random Forest Metrics: {rf_metrics}")
    
    # XGBoost with hyperparameter tuning
    xgb_model, xgb_metrics = ml_pipeline.run_experiment(
        experiment_name="customer_churn_xgboost",
        dataset_path="datasets/raw/customer_churn_dataset.csv",
        target_column="target",
        model_type="xgboost_classifier",
        hyperparameters={
            'n_estimators': 100,
            'max_depth': 6,
            'learning_rate': 0.1,
            'random_state': 42
        },
        task_type='classification',
        hyperparameter_tuning=True,
        tuning_params={
            'n_estimators': [50, 100, 200],
            'max_depth': [3, 6, 10],
            'learning_rate': [0.01, 0.1, 0.2]
        },
        notes="XGBoost with hyperparameter tuning",
        tags=['tuned', 'xgboost', 'churn']
    )
    
    print(f"XGBoost Metrics: {xgb_metrics}")
    
    # 2. Model comparison
    print("\n=== Running Model Comparison ===")
    
    models_to_compare = [
        {
            'model_type': 'random_forest_classifier',
            'hyperparameters': {'n_estimators': 100, 'random_state': 42}
        },
        {
            'model_type': 'logistic_regression',
            'hyperparameters': {'random_state': 42, 'max_iter': 1000}
        },
        {
            'model_type': 'xgboost_classifier',
            'hyperparameters': {'n_estimators': 100, 'random_state': 42}
        }
    ]
    
    comparison_results = ml_pipeline.compare_models(
        experiment_name="churn_model_comparison",
        dataset_path="datasets/raw/customer_churn_dataset.csv",
        target_column="target",
        models_to_compare=models_to_compare,
        task_type='classification'
    )
    
    print("Model Comparison Results:")
    print(comparison_results[['model_type', 'accuracy', 'f1_score', 'roc_auc']].to_string())
    
    # 3. Regression experiment
    print("\n=== Running Regression Experiments ===")
    
    # House price prediction
    reg_model, reg_metrics = ml_pipeline.run_experiment(
        experiment_name="house_price_prediction",
        dataset_path="datasets/raw/house_price_dataset.csv",
        target_column="target",
        model_type="random_forest_regressor",
        hyperparameters={
            'n_estimators': 100,
            'max_depth': 15,
            'random_state': 42
        },
        task_type='regression',
        notes="House price prediction using Random Forest",
        tags=['regression', 'house_prices', 'random_forest']
    )
    
    print(f"Regression Metrics: {reg_metrics}")
    
    # 4. List all experiments
    print("\n=== Experiment Summary ===")
    
    all_experiments = ml_pipeline.tracker.list_experiments()
    
    print(f"Total experiments run: {len(all_experiments)}")
    for exp in all_experiments[:5]:  # Show last 5 experiments
        print(f"- {exp['experiment_name']} ({exp['model_type']}) - Status: {exp['status']}")
        if exp.get('metrics'):
            main_metric = list(exp['metrics'].keys())[0] if exp['metrics'] else 'N/A'
            main_value = exp['metrics'].get(main_metric, 'N/A')
            print(f"  {main_metric}: {main_value}")
    
    # 5. Compare specific experiments
    print("\n=== Experiment Comparison ===")
    
    # Get run IDs for comparison
    churn_experiments = [exp for exp in all_experiments if 'churn' in exp['experiment_name']]
    if len(churn_experiments) >= 2:
        run_ids = [exp['run_id'] for exp in churn_experiments[:3]]
        comparison_df = ml_pipeline.tracker.compare_experiments(run_ids)
        
        print("Churn Prediction Experiments Comparison:")
        metric_columns = [col for col in comparison_df.columns if col.startswith('metric_')]
        display_columns = ['experiment_name', 'model_type'] + metric_columns
        print(comparison_df[display_columns].to_string())

if __name__ == "__main__":
    run_complete_ml_workflow()
```

## Step 5: Model Registry and Deployment

### Model Registry System

```python
# src/model_registry.py
import lakefs
import lakefs_spec
import pandas as pd
import numpy as np
import json
import joblib
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
import io
import uuid

logger = logging.getLogger(__name__)

class ModelStage(Enum):
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"

@dataclass
class RegisteredModel:
    """Represents a registered model in the model registry"""
    model_name: str
    model_version: str
    model_stage: ModelStage
    model_path: str
    metadata_path: str
    experiment_run_id: str
    created_at: datetime
    created_by: str
    description: str
    tags: List[str] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    model_signature: Optional[Dict[str, Any]] = None
    deployment_info: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'model_name': self.model_name,
            'model_version': self.model_version,
            'model_stage': self.model_stage.value,
            'model_path': self.model_path,
            'metadata_path': self.metadata_path,
            'experiment_run_id': self.experiment_run_id,
            'created_at': self.created_at.isoformat(),
            'created_by': self.created_by,
            'description': self.description,
            'tags': self.tags,
            'metrics': self.metrics,
            'parameters': self.parameters,
            'model_signature': self.model_signature,
            'deployment_info': self.deployment_info
        }

class ModelRegistry:
    """Centralized model registry using lakeFS"""
    
    def __init__(self, repository_name: str = 'ml-experiments'):
        self.client = lakefs.Client(
            host='http://localhost:8000',
            username='AKIAIOSFODNN7EXAMPLE',
            password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        )
        self.repo = self.client.repositories.get(repository_name)
        self.fs = lakefs_spec.LakeFSFileSystem(
            host='http://localhost:8000',
            username='AKIAIOSFODNN7EXAMPLE',
            password='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        )
        self.logger = logging.getLogger(__name__)
    
    def register_model(
        self,
        model_name: str,
        experiment_run_id: str,
        model_artifact_name: str = "trained_model",
        description: str = "",
        tags: List[str] = None,
        stage: ModelStage = ModelStage.STAGING,
        created_by: str = "unknown"
    ) -> RegisteredModel:
        """Register a model from an experiment run"""
        
        # Load experiment metadata
        experiments_branch = self.repo.branches.get('experiments')
        
        try:
            # Get experiment metadata
            exp_metadata_obj = experiments_branch.objects.get(f'experiments/runs/{experiment_run_id}/metadata.json')
            exp_metadata = json.loads(exp_metadata_obj.reader().read().decode())
            
            # Get model metadata
            model_metadata_obj = experiments_branch.objects.get(f'experiments/runs/{experiment_run_id}/models/{model_artifact_name}_metadata.json')
            model_metadata = json.loads(model_metadata_obj.reader().read().decode())
            
        except Exception as e:
            raise ValueError(f"Could not load experiment or model metadata: {e}")
        
        # Generate model version
        model_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Copy model to registry
        registry_branch = self._get_or_create_registry_branch()
        
        # Source paths
        source_model_path = f'experiments/runs/{experiment_run_id}/models/{model_artifact_name}.joblib'
        source_metadata_path = f'experiments/runs/{experiment_run_id}/models/{model_artifact_name}_metadata.json'
        
        # Registry paths
        registry_model_path = f'models/registered/{model_name}/{model_version}/model.joblib'
        registry_metadata_path = f'models/registered/{model_name}/{model_version}/metadata.json'
        
        # Copy model file
        model_obj = experiments_branch.objects.get(source_model_path)
        model_data = model_obj.reader().read()
        
        registry_branch.objects.upload(
            path=registry_model_path,
            data=model_data,
            content_type='application/octet-stream'
        )
        
        # Copy model metadata
        registry_branch.objects.upload(
            path=registry_metadata_path,
            data=json.dumps(model_metadata, indent=2).encode(),
            content_type='application/json'
        )
        
        # Create registered model entry
        registered_model = RegisteredModel(
            model_name=model_name,
            model_version=model_version,
            model_stage=stage,
            model_path=registry_model_path,
            metadata_path=registry_metadata_path,
            experiment_run_id=experiment_run_id,
            created_at=datetime.now(),
            created_by=created_by,
            description=description,
            tags=tags or [],
            metrics=exp_metadata.get('metrics', {}),
            parameters=exp_metadata.get('parameters', {}),
            model_signature=self._extract_model_signature(model_metadata)
        )
        
        # Save registry entry
        registry_entry_path = f'models/registered/{model_name}/{model_version}/registry_entry.json'
        registry_branch.objects.upload(
            path=registry_entry_path,
            data=json.dumps(registered_model.to_dict(), indent=2).encode(),
            content_type='application/json'
        )
        
        # Update model index
        self._update_model_index(registry_branch, registered_model)
        
        # Commit registration
        commit = registry_branch.commits.create(
            message=f"Register model: {model_name} version {model_version}",
            metadata={
                'model_name': model_name,
                'model_version': model_version,
                'stage': stage.value,
                'experiment_run_id': experiment_run_id
            }
        )
        
        self.logger.info(f"Registered model: {model_name} version {model_version}")
        
        return registered_model
    
    def list_models(self, model_name: Optional[str] = None, stage: Optional[ModelStage] = None) -> List[RegisteredModel]:
        """List registered models"""
        
        models = []
        
        try:
            registry_branch = self.repo.branches.get('production')
            
            # Get model index
            index_obj = registry_branch.objects.get('models/model_index.json')
            model_index = json.loads(index_obj.reader().read().decode())
            
            for model_entry in model_index.get('models', []):
                # Filter by model name
                if model_name and model_entry['model_name'] != model_name:
                    continue
                
                # Filter by stage
                if stage and ModelStage(model_entry['model_stage']) != stage:
                    continue
                
                # Convert to RegisteredModel object
                model_entry['created_at'] = datetime.fromisoformat(model_entry['created_at'])
                model_entry['model_stage'] = ModelStage(model_entry['model_stage'])
                
                registered_model = RegisteredModel(**model_entry)
                models.append(registered_model)
            
        except Exception as e:
            self.logger.error(f"Error listing models: {e}")
        
        return models
    
    def get_model(self, model_name: str, model_version: Optional[str] = None, stage: Optional[ModelStage] = None):
        """Get a specific model"""
        
        models = self.list_models(model_name=model_name, stage=stage)
        
        if not models:
            raise ValueError(f"No models found for {model_name}")
        
        if model_version:
            # Find specific version
            for model in models:
                if model.model_version == model_version:
                    return model
            raise ValueError(f"Model version {model_version} not found for {model_name}")
        else:
            # Return latest version
            models.sort(key=lambda x: x.created_at, reverse=True)
            return models[0]
    
    def load_model(self, model_name: str, model_version: Optional[str] = None, stage: Optional[ModelStage] = None):
        """Load a model from the registry"""
        
        registered_model = self.get_model(model_name, model_version, stage)
        
        try:
            registry_branch = self.repo.branches.get('production')
            model_obj = registry_branch.objects.get(registered_model.model_path)
            model_data = model_obj.reader().read()
            
            # Load model from bytes
            model = joblib.load(io.BytesIO(model_data))
            
            self.logger.info(f"Loaded model: {model_name} version {registered_model.model_version}")
            return model, registered_model
            
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            raise
    
    def promote_model(self, model_name: str, model_version: str, target_stage: ModelStage) -> RegisteredModel:
        """Promote a model to a different stage"""
        
        # Get current model
        current_model = self.get_model(model_name, model_version)
        
        # Update stage
        current_model.model_stage = target_stage
        
        # Update registry
        registry_branch = self._get_or_create_registry_branch()
        
        # Update registry entry
        registry_entry_path = f'models/registered/{model_name}/{model_version}/registry_entry.json'
        registry_branch.objects.upload(
            path=registry_entry_path,
            data=json.dumps(current_model.to_dict(), indent=2).encode(),
            content_type='application/json'
        )
        
        # Update model index
        self._update_model_index(registry_branch, current_model)
        
        # Commit promotion
        commit = registry_branch.commits.create(
            message=f"Promote model: {model_name} version {model_version} to {target_stage.value}",
            metadata={
                'model_name': model_name,
                'model_version': model_version,
                'new_stage': target_stage.value,
                'action': 'promotion'
            }
        )
        
        # If promoting to production, merge to production branch
        if target_stage == ModelStage.PRODUCTION:
            try:
                production_branch = self.repo.branches.get('production')
                production_branch.merge(
                    source_reference=registry_branch.id,
                    message=f"Deploy model to production: {model_name} version {model_version}"
                )
                self.logger.info(f"Model deployed to production: {model_name} version {model_version}")
            except Exception as e:
                self.logger.warning(f"Could not merge to production branch: {e}")
        
        self.logger.info(f"Promoted model: {model_name} version {model_version} to {target_stage.value}")
        
        return current_model
    
    def archive_model(self, model_name: str, model_version: str) -> RegisteredModel:
        """Archive a model"""
        return self.promote_model(model_name, model_version, ModelStage.ARCHIVED)
    
    def delete_model(self, model_name: str, model_version: str):
        """Delete a model from the registry"""
        
        registry_branch = self._get_or_create_registry_branch()
        
        # Delete model files
        model_dir = f'models/registered/{model_name}/{model_version}/'
        
        try:
            # List all objects in the model directory
            objects = registry_branch.objects.list(prefix=model_dir)
            
            for obj in objects:
                registry_branch.objects.delete(obj.path)
            
            # Update model index
            self._remove_from_model_index(registry_branch, model_name, model_version)
            
            # Commit deletion
            commit = registry_branch.commits.create(
                message=f"Delete model: {model_name} version {model_version}",
                metadata={
                    'model_name': model_name,
                    'model_version': model_version,
                    'action': 'deletion'
                }
            )
            
            self.logger.info(f"Deleted model: {model_name} version {model_version}")
            
        except Exception as e:
            self.logger.error(f"Error deleting model: {e}")
            raise
    
    def _get_or_create_registry_branch(self):
        """Get or create the model registry branch"""
        try:
            return self.repo.branches.get('model-registry')
        except:
            return self.repo.branches.create('model-registry', source_reference='production')
    
    def _extract_model_signature(self, model_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Extract model signature from metadata"""
        return {
            'feature_columns': model_metadata.get('custom_metadata', {}).get('feature_columns', []),
            'target_column': model_metadata.get('custom_metadata', {}).get('target_column'),
            'model_class': model_metadata.get('custom_metadata', {}).get('model_class')
        }
    
    def _update_model_index(self, branch, registered_model: RegisteredModel):
        """Update the model index with new/updated model"""
        
        try:
            # Load existing index
            index_obj = branch.objects.get('models/model_index.json')
            model_index = json.loads(index_obj.reader().read().decode())
        except:
            # Create new index
            model_index = {'models': [], 'last_updated': datetime.now().isoformat()}
        
        # Remove existing entry for this model version
        model_index['models'] = [
            m for m in model_index['models'] 
            if not (m['model_name'] == registered_model.model_name and m['model_version'] == registered_model.model_version)
        ]
        
        # Add new entry
        model_index['models'].append(registered_model.to_dict())
        model_index['last_updated'] = datetime.now().isoformat()
        
        # Save updated index
        branch.objects.upload(
            path='models/model_index.json',
            data=json.dumps(model_index, indent=2).encode(),
            content_type='application/json'
        )
    
    def _remove_from_model_index(self, branch, model_name: str, model_version: str):
        """Remove model from index"""
        
        try:
            # Load existing index
            index_obj = branch.objects.get('models/model_index.json')
            model_index = json.loads(index_obj.reader().read().decode())
            
            # Remove entry
            model_index['models'] = [
                m for m in model_index['models'] 
                if not (m['model_name'] == model_name and m['model_version'] == model_version)
            ]
            model_index['last_updated'] = datetime.now().isoformat()
            
            # Save updated index
            branch.objects.upload(
                path='models/model_index.json',
                data=json.dumps(model_index, indent=2).encode(),
                content_type='application/json'
            )
            
        except Exception as e:
            self.logger.warning(f"Could not update model index: {e}")

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create model registry
    registry = ModelRegistry()
    
    # Register a model (assuming we have an experiment run)
    # registered_model = registry.register_model(
    #     model_name="customer_churn_predictor",
    #     experiment_run_id="some-run-id",
    #     description="Random Forest model for customer churn prediction",
    #     tags=['churn', 'random_forest', 'production_ready']
    # )
    
    # List all models
    models = registry.list_models()
    print(f"Found {len(models)} registered models")
    
    for model in models:
        print(f"- {model.model_name} {model.model_version} ({model.model_stage.value})")
```#
# Step 6: A/B Testing and Model Comparison

### A/B Testing Framework

```python
# src/ab_testing.py
import lakefs
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass, field
from scipy import stats
import json
import uuid

from src.model_registry import ModelRegistry, ModelStage

logger = logging.getLogger(__name__)

@dataclass
class ABTestConfig:
    """Configuration for A/B test"""
    test_name: str
    description: str
    model_a_name: str
    model_a_version: str
    model_b_name: str
    model_b_version: str
    traffic_split: float = 0.5  # Percentage of traffic to model B
    success_metric: str = 'accuracy'
    minimum_sample_size: int = 1000
    significance_level: float = 0.05
    test_duration_days: int = 7
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'test_name': self.test_name,
            'description': self.description,
            'model_a_name': self.model_a_name,
            'model_a_version': self.model_a_version,
            'model_b_name': self.model_b_name,
            'model_b_version': self.model_b_version,
            'traffic_split': self.traffic_split,
            'success_metric': self.success_metric,
            'minimum_sample_size': self.minimum_sample_size,
            'significance_level': self.significance_level,
            'test_duration_days': self.test_duration_days
        }

@dataclass
class ABTestResult:
    """Results of an A/B test"""
    test_name: str
    start_date: datetime
    end_date: datetime
    model_a_metrics: Dict[str, float]
    model_b_metrics: Dict[str, float]
    sample_size_a: int
    sample_size_b: int
    statistical_significance: bool
    p_value: float
    confidence_interval: Tuple[float, float]
    winner: str  # 'model_a', 'model_b', or 'inconclusive'
    recommendation: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'test_name': self.test_name,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
            'model_a_metrics': self.model_a_metrics,
            'model_b_metrics': self.model_b_metrics,
            'sample_size_a': self.sample_size_a,
            'sample_size_b': self.sample_size_b,
            'statistical_significance': self.statistical_significance,
            'p_value': self.p_value,
            'confidence_interval': list(self.confidence_interval),
            'winner': self.winner,
            'recommendation': self.recommendation
        }

class ABTestingFramework:
    """A/B testing framework for ML models"""
    
    def __init__(self, repository_name: str = 'ml-experiments'):
        self.model_registry = ModelRegistry(repository_name)
        self.repo = self.model_registry.repo
        self.logger = logging.getLogger(__name__)
    
    def create_ab_test(self, config: ABTestConfig) -> str:
        """Create a new A/B test"""
        
        # Validate models exist
        try:
            model_a = self.model_registry.get_model(config.model_a_name, config.model_a_version)
            model_b = self.model_registry.get_model(config.model_b_name, config.model_b_version)
        except Exception as e:
            raise ValueError(f"Could not find specified models: {e}")
        
        # Create test branch
        test_id = str(uuid.uuid4())
        test_branch_name = f"ab-test/{config.test_name}/{test_id[:8]}"
        
        try:
            test_branch = self.repo.branches.create(test_branch_name, source_reference='production')
        except Exception as e:
            raise ValueError(f"Could not create test branch: {e}")
        
        # Save test configuration
        test_config_path = f"ab_tests/{config.test_name}/config.json"
        test_branch.objects.upload(
            path=test_config_path,
            data=json.dumps(config.to_dict(), indent=2).encode(),
            content_type='application/json'
        )
        
        # Create test metadata
        test_metadata = {
            'test_id': test_id,
            'test_name': config.test_name,
            'status': 'created',
            'created_at': datetime.now().isoformat(),
            'branch_name': test_branch_name,
            'config': config.to_dict()
        }
        
        test_metadata_path = f"ab_tests/{config.test_name}/metadata.json"
        test_branch.objects.upload(
            path=test_metadata_path,
            data=json.dumps(test_metadata, indent=2).encode(),
            content_type='application/json'
        )
        
        # Commit test creation
        commit = test_branch.commits.create(
            message=f"Create A/B test: {config.test_name}",
            metadata={
                'test_name': config.test_name,
                'test_id': test_id,
                'model_a': f"{config.model_a_name}:{config.model_a_version}",
                'model_b': f"{config.model_b_name}:{config.model_b_version}",
                'action': 'create_ab_test'
            }
        )
        
        self.logger.info(f"Created A/B test: {config.test_name} (ID: {test_id})")
        
        return test_id
    
    def simulate_ab_test(
        self,
        test_name: str,
        test_dataset: pd.DataFrame,
        target_column: str,
        feature_columns: List[str]
    ) -> ABTestResult:
        """Simulate an A/B test using historical data"""
        
        # Load test configuration
        try:
            production_branch = self.repo.branches.get('production')
            config_obj = production_branch.objects.get(f"ab_tests/{test_name}/config.json")
            config_data = json.loads(config_obj.reader().read().decode())
            config = ABTestConfig(**config_data)
        except Exception as e:
            raise ValueError(f"Could not load test configuration: {e}")
        
        # Load models
        model_a, reg_model_a = self.model_registry.load_model(config.model_a_name, config.model_a_version)
        model_b, reg_model_b = self.model_registry.load_model(config.model_b_name, config.model_b_version)
        
        # Prepare data
        X = test_dataset[feature_columns]
        y_true = test_dataset[target_column]
        
        # Split data according to traffic split
        n_samples = len(test_dataset)
        n_b = int(n_samples * config.traffic_split)
        n_a = n_samples - n_b
        
        # Random assignment (in practice, this would be based on user ID hash)
        np.random.seed(42)  # For reproducibility
        assignment = np.random.choice(['A', 'B'], size=n_samples, p=[1-config.traffic_split, config.traffic_split])
        
        # Get predictions
        y_pred_a = model_a.predict(X)
        y_pred_b = model_b.predict(X)
        
        # Calculate metrics for each group
        a_indices = assignment == 'A'
        b_indices = assignment == 'B'
        
        metrics_a = self._calculate_metrics(y_true[a_indices], y_pred_a[a_indices])
        metrics_b = self._calculate_metrics(y_true[b_indices], y_pred_b[b_indices])
        
        # Statistical significance test
        success_metric = config.success_metric
        
        if success_metric in metrics_a and success_metric in metrics_b:
            # For accuracy/precision/recall, we can use proportion test
            if success_metric in ['accuracy', 'precision', 'recall', 'f1_score']:
                # Convert to success/failure counts
                successes_a = int(metrics_a[success_metric] * sum(a_indices))
                successes_b = int(metrics_b[success_metric] * sum(b_indices))
                
                # Two-proportion z-test
                stat, p_value = self._two_proportion_test(
                    successes_a, sum(a_indices),
                    successes_b, sum(b_indices)
                )
            else:
                # For continuous metrics, use t-test
                # This is simplified - in practice you'd need the raw values
                stat, p_value = stats.ttest_ind(
                    np.random.normal(metrics_a[success_metric], 0.1, sum(a_indices)),
                    np.random.normal(metrics_b[success_metric], 0.1, sum(b_indices))
                )
        else:
            p_value = 1.0
            stat = 0.0
        
        # Determine winner
        is_significant = p_value < config.significance_level
        
        if is_significant:
            if metrics_b[success_metric] > metrics_a[success_metric]:
                winner = 'model_b'
                recommendation = f"Model B ({config.model_b_name}) performs significantly better"
            else:
                winner = 'model_a'
                recommendation = f"Model A ({config.model_a_name}) performs significantly better"
        else:
            winner = 'inconclusive'
            recommendation = "No statistically significant difference found"
        
        # Calculate confidence interval (simplified)
        diff = metrics_b[success_metric] - metrics_a[success_metric]
        se = np.sqrt(metrics_a[success_metric] * (1 - metrics_a[success_metric]) / sum(a_indices) +
                     metrics_b[success_metric] * (1 - metrics_b[success_metric]) / sum(b_indices))
        ci_lower = diff - 1.96 * se
        ci_upper = diff + 1.96 * se
        
        # Create result
        result = ABTestResult(
            test_name=test_name,
            start_date=datetime.now() - timedelta(days=config.test_duration_days),
            end_date=datetime.now(),
            model_a_metrics=metrics_a,
            model_b_metrics=metrics_b,
            sample_size_a=sum(a_indices),
            sample_size_b=sum(b_indices),
            statistical_significance=is_significant,
            p_value=p_value,
            confidence_interval=(ci_lower, ci_upper),
            winner=winner,
            recommendation=recommendation
        )
        
        # Save test results
        self._save_test_results(test_name, result)
        
        return result
    
    def _calculate_metrics(self, y_true, y_pred) -> Dict[str, float]:
        """Calculate standard ML metrics"""
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
        
        try:
            metrics = {
                'accuracy': accuracy_score(y_true, y_pred),
                'precision': precision_score(y_true, y_pred, average='weighted', zero_division=0),
                'recall': recall_score(y_true, y_pred, average='weighted', zero_division=0),
                'f1_score': f1_score(y_true, y_pred, average='weighted', zero_division=0)
            }
        except Exception as e:
            self.logger.warning(f"Error calculating metrics: {e}")
            metrics = {'accuracy': 0.0, 'precision': 0.0, 'recall': 0.0, 'f1_score': 0.0}
        
        return metrics
    
    def _two_proportion_test(self, x1, n1, x2, n2):
        """Two-proportion z-test"""
        p1 = x1 / n1
        p2 = x2 / n2
        p_pool = (x1 + x2) / (n1 + n2)
        
        se = np.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
        z = (p2 - p1) / se
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        return z, p_value
    
    def _save_test_results(self, test_name: str, result: ABTestResult):
        """Save A/B test results"""
        
        try:
            production_branch = self.repo.branches.get('production')
            
            results_path = f"ab_tests/{test_name}/results.json"
            production_branch.objects.upload(
                path=results_path,
                data=json.dumps(result.to_dict(), indent=2).encode(),
                content_type='application/json'
            )
            
            # Update test metadata
            metadata_obj = production_branch.objects.get(f"ab_tests/{test_name}/metadata.json")
            metadata = json.loads(metadata_obj.reader().read().decode())
            metadata['status'] = 'completed'
            metadata['completed_at'] = datetime.now().isoformat()
            metadata['winner'] = result.winner
            
            production_branch.objects.upload(
                path=f"ab_tests/{test_name}/metadata.json",
                data=json.dumps(metadata, indent=2).encode(),
                content_type='application/json'
            )
            
            # Commit results
            commit = production_branch.commits.create(
                message=f"A/B test results: {test_name}",
                metadata={
                    'test_name': test_name,
                    'winner': result.winner,
                    'p_value': str(result.p_value),
                    'action': 'ab_test_results'
                }
            )
            
            self.logger.info(f"Saved A/B test results: {test_name}")
            
        except Exception as e:
            self.logger.error(f"Error saving test results: {e}")
    
    def list_ab_tests(self) -> List[Dict[str, Any]]:
        """List all A/B tests"""
        
        tests = []
        
        try:
            production_branch = self.repo.branches.get('production')
            
            # List all test directories
            test_objects = production_branch.objects.list(prefix='ab_tests/')
            
            test_names = set()
            for obj in test_objects:
                path_parts = obj.path.split('/')
                if len(path_parts) >= 2:
                    test_names.add(path_parts[1])
            
            # Load metadata for each test
            for test_name in test_names:
                try:
                    metadata_obj = production_branch.objects.get(f'ab_tests/{test_name}/metadata.json')
                    metadata = json.loads(metadata_obj.reader().read().decode())
                    tests.append(metadata)
                except Exception as e:
                    self.logger.warning(f"Could not load metadata for test {test_name}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error listing A/B tests: {e}")
        
        return tests
    
    def get_test_results(self, test_name: str) -> Optional[ABTestResult]:
        """Get results for a specific A/B test"""
        
        try:
            production_branch = self.repo.branches.get('production')
            results_obj = production_branch.objects.get(f'ab_tests/{test_name}/results.json')
            results_data = json.loads(results_obj.reader().read().decode())
            
            # Convert datetime strings back to datetime objects
            results_data['start_date'] = datetime.fromisoformat(results_data['start_date'])
            results_data['end_date'] = datetime.fromisoformat(results_data['end_date'])
            results_data['confidence_interval'] = tuple(results_data['confidence_interval'])
            
            return ABTestResult(**results_data)
            
        except Exception as e:
            self.logger.error(f"Error loading test results: {e}")
            return None

# Example usage and complete A/B testing workflow
def run_ab_testing_example():
    """Demonstrate A/B testing workflow"""
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize A/B testing framework
    ab_framework = ABTestingFramework()
    
    # Create A/B test configuration
    test_config = ABTestConfig(
        test_name="churn_model_comparison",
        description="Compare Random Forest vs XGBoost for customer churn prediction",
        model_a_name="customer_churn_predictor",
        model_a_version="v20240115_120000",  # Assuming these models exist
        model_b_name="customer_churn_xgboost",
        model_b_version="v20240115_130000",
        traffic_split=0.5,
        success_metric='accuracy',
        minimum_sample_size=1000,
        significance_level=0.05,
        test_duration_days=7
    )
    
    # Create A/B test
    try:
        test_id = ab_framework.create_ab_test(test_config)
        print(f"Created A/B test: {test_id}")
    except Exception as e:
        print(f"Could not create A/B test: {e}")
        return
    
    # Load test dataset (in practice, this would be production data)
    try:
        test_dataset = ab_framework.model_registry.fs
        # For demo, we'll create synthetic data
        np.random.seed(42)
        n_samples = 2000
        
        test_data = pd.DataFrame({
            'feature_0': np.random.randn(n_samples),
            'feature_1': np.random.randn(n_samples),
            'feature_2': np.random.randn(n_samples),
            'target': np.random.choice([0, 1], n_samples, p=[0.7, 0.3])
        })
        
        feature_columns = ['feature_0', 'feature_1', 'feature_2']
        
        # Simulate A/B test
        # result = ab_framework.simulate_ab_test(
        #     test_name="churn_model_comparison",
        #     test_dataset=test_data,
        #     target_column='target',
        #     feature_columns=feature_columns
        # )
        
        # print(f"A/B Test Results:")
        # print(f"Winner: {result.winner}")
        # print(f"Statistical Significance: {result.statistical_significance}")
        # print(f"P-value: {result.p_value:.4f}")
        # print(f"Model A Accuracy: {result.model_a_metrics['accuracy']:.4f}")
        # print(f"Model B Accuracy: {result.model_b_metrics['accuracy']:.4f}")
        # print(f"Recommendation: {result.recommendation}")
        
    except Exception as e:
        print(f"Could not run A/B test simulation: {e}")
    
    # List all A/B tests
    all_tests = ab_framework.list_ab_tests()
    print(f"\nAll A/B Tests ({len(all_tests)}):")
    for test in all_tests:
        print(f"- {test['test_name']} (Status: {test.get('status', 'unknown')})")

if __name__ == "__main__":
    run_ab_testing_example()
```

## Step 7: Production MLOps Pipeline

### Complete MLOps Workflow

```python
# src/mlops_pipeline.py
import lakefs
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import json
import schedule
import time
from dataclasses import dataclass
import smtplib
from email.mime.text import MIMEText

from src.experiment_tracker import MLExperimentTracker
from src.model_registry import ModelRegistry, ModelStage
from src.ab_testing import ABTestingFramework
from src.ml_pipeline import MLPipeline

logger = logging.getLogger(__name__)

@dataclass
class MLOpsConfig:
    """Configuration for MLOps pipeline"""
    model_name: str
    dataset_path: str
    target_column: str
    feature_columns: List[str]
    model_type: str
    hyperparameters: Dict[str, Any]
    retraining_schedule: str  # cron-like schedule
    performance_threshold: float
    data_drift_threshold: float
    alert_recipients: List[str]
    auto_deploy: bool = False

class MLOpsPipeline:
    """Complete MLOps pipeline with automated retraining and deployment"""
    
    def __init__(self, repository_name: str = 'ml-experiments'):
        self.ml_pipeline = MLPipeline(repository_name)
        self.model_registry = ModelRegistry(repository_name)
        self.ab_framework = ABTestingFramework(repository_name)
        self.repo = self.ml_pipeline.repo
        self.logger = logging.getLogger(__name__)
        
        # Pipeline state
        self.configs: Dict[str, MLOpsConfig] = {}
        self.monitoring_data: Dict[str, List[Dict[str, Any]]] = {}
    
    def register_model_pipeline(self, config: MLOpsConfig):
        """Register a model for automated MLOps pipeline"""
        
        self.configs[config.model_name] = config
        self.monitoring_data[config.model_name] = []
        
        # Schedule retraining
        if config.retraining_schedule:
            schedule.every().day.at(config.retraining_schedule).do(
                self._retrain_model, config.model_name
            )
        
        self.logger.info(f"Registered MLOps pipeline for model: {config.model_name}")
    
    def monitor_model_performance(self, model_name: str, predictions: pd.DataFrame, actuals: pd.DataFrame):
        """Monitor model performance in production"""
        
        if model_name not in self.configs:
            raise ValueError(f"Model {model_name} not registered in MLOps pipeline")
        
        config = self.configs[model_name]
        
        # Calculate performance metrics
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'accuracy': accuracy_score(actuals, predictions),
            'precision': precision_score(actuals, predictions, average='weighted', zero_division=0),
            'recall': recall_score(actuals, predictions, average='weighted', zero_division=0),
            'f1_score': f1_score(actuals, predictions, average='weighted', zero_division=0),
            'sample_size': len(predictions)
        }
        
        # Store monitoring data
        self.monitoring_data[model_name].append(metrics)
        
        # Check for performance degradation
        if metrics['accuracy'] < config.performance_threshold:
            self._trigger_performance_alert(model_name, metrics)
            
            if config.auto_deploy:
                self._trigger_retraining(model_name)
        
        # Save monitoring data
        self._save_monitoring_data(model_name)
        
        self.logger.info(f"Monitored performance for {model_name}: accuracy={metrics['accuracy']:.4f}")
    
    def detect_data_drift(self, model_name: str, new_data: pd.DataFrame) -> bool:
        """Detect data drift in production data"""
        
        if model_name not in self.configs:
            raise ValueError(f"Model {model_name} not registered in MLOps pipeline")
        
        config = self.configs[model_name]
        
        try:
            # Load training data for comparison
            training_data = self.ml_pipeline.load_dataset(config.dataset_path)
            training_features = training_data[config.feature_columns]
            new_features = new_data[config.feature_columns]
            
            # Simple drift detection using statistical tests
            drift_detected = False
            drift_scores = {}
            
            for column in config.feature_columns:
                if column in training_features.columns and column in new_features.columns:
                    # Kolmogorov-Smirnov test for distribution comparison
                    from scipy.stats import ks_2samp
                    
                    statistic, p_value = ks_2samp(
                        training_features[column].dropna(),
                        new_features[column].dropna()
                    )
                    
                    drift_scores[column] = {
                        'ks_statistic': statistic,
                        'p_value': p_value,
                        'drift_detected': p_value < 0.05  # Significant difference
                    }
                    
                    if p_value < 0.05:
                        drift_detected = True
            
            # Log drift detection results
            drift_result = {
                'timestamp': datetime.now().isoformat(),
                'model_name': model_name,
                'drift_detected': drift_detected,
                'drift_scores': drift_scores
            }
            
            self._save_drift_detection_results(model_name, drift_result)
            
            if drift_detected:
                self._trigger_drift_alert(model_name, drift_result)
                
                if config.auto_deploy:
                    self._trigger_retraining(model_name)
            
            return drift_detected
            
        except Exception as e:
            self.logger.error(f"Error detecting data drift for {model_name}: {e}")
            return False
    
    def _retrain_model(self, model_name: str):
        """Retrain a model automatically"""
        
        if model_name not in self.configs:
            self.logger.error(f"Model {model_name} not found in configurations")
            return
        
        config = self.configs[model_name]
        
        try:
            self.logger.info(f"Starting automatic retraining for {model_name}")
            
            # Run experiment with updated data
            model, metrics = self.ml_pipeline.run_experiment(
                experiment_name=f"{model_name}_retrain_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                dataset_path=config.dataset_path,
                target_column=config.target_column,
                model_type=config.model_type,
                hyperparameters=config.hyperparameters,
                feature_columns=config.feature_columns,
                notes=f"Automatic retraining triggered for {model_name}",
                tags=['retrain', 'automated', model_name]
            )
            
            # Get the experiment run ID (this would need to be returned from run_experiment)
            # For now, we'll simulate this
            experiment_run_id = "simulated-run-id"
            
            # Register new model version
            registered_model = self.model_registry.register_model(
                model_name=f"{model_name}_retrained",
                experiment_run_id=experiment_run_id,
                description=f"Automatically retrained version of {model_name}",
                tags=['retrained', 'automated'],
                stage=ModelStage.STAGING,
                created_by='mlops_pipeline'
            )
            
            # Compare with current production model
            current_production_models = self.model_registry.list_models(
                model_name=model_name,
                stage=ModelStage.PRODUCTION
            )
            
            if current_production_models:
                # Set up A/B test
                from src.ab_testing import ABTestConfig
                
                ab_config = ABTestConfig(
                    test_name=f"{model_name}_retrain_test",
                    description=f"A/B test for retrained {model_name}",
                    model_a_name=current_production_models[0].model_name,
                    model_a_version=current_production_models[0].model_version,
                    model_b_name=registered_model.model_name,
                    model_b_version=registered_model.model_version,
                    traffic_split=0.1,  # Start with 10% traffic to new model
                    success_metric='accuracy'
                )
                
                test_id = self.ab_framework.create_ab_test(ab_config)
                
                self.logger.info(f"Created A/B test for retrained model: {test_id}")
                
                # Send notification
                self._send_notification(
                    subject=f"Model Retrained: {model_name}",
                    message=f"""
                    Model {model_name} has been automatically retrained.
                    
                    New model metrics:
                    {json.dumps(metrics, indent=2)}
                    
                    A/B test created: {test_id}
                    
                    The new model is staged for testing with 10% traffic.
                    """,
                    recipients=config.alert_recipients
                )
            
        except Exception as e:
            self.logger.error(f"Error retraining model {model_name}: {e}")
            self._send_notification(
                subject=f"Model Retraining Failed: {model_name}",
                message=f"Automatic retraining failed for {model_name}: {str(e)}",
                recipients=config.alert_recipients
            )
    
    def _trigger_performance_alert(self, model_name: str, metrics: Dict[str, Any]):
        """Trigger alert for performance degradation"""
        
        config = self.configs[model_name]
        
        message = f"""
        Performance Alert: {model_name}
        
        Current accuracy: {metrics['accuracy']:.4f}
        Threshold: {config.performance_threshold:.4f}
        
        Recent performance metrics:
        {json.dumps(metrics, indent=2)}
        
        Consider retraining the model or investigating data quality issues.
        """
        
        self._send_notification(
            subject=f"Performance Alert: {model_name}",
            message=message,
            recipients=config.alert_recipients
        )
    
    def _trigger_drift_alert(self, model_name: str, drift_result: Dict[str, Any]):
        """Trigger alert for data drift"""
        
        config = self.configs[model_name]
        
        message = f"""
        Data Drift Alert: {model_name}
        
        Data drift detected in production data.
        
        Drift detection results:
        {json.dumps(drift_result['drift_scores'], indent=2)}
        
        Consider retraining the model with recent data.
        """
        
        self._send_notification(
            subject=f"Data Drift Alert: {model_name}",
            message=message,
            recipients=config.alert_recipients
        )
    
    def _trigger_retraining(self, model_name: str):
        """Trigger model retraining"""
        
        self.logger.info(f"Triggering retraining for {model_name}")
        self._retrain_model(model_name)
    
    def _save_monitoring_data(self, model_name: str):
        """Save monitoring data to lakeFS"""
        
        try:
            production_branch = self.repo.branches.get('production')
            
            monitoring_path = f"monitoring/{model_name}/performance_metrics.json"
            monitoring_data = {
                'model_name': model_name,
                'metrics': self.monitoring_data[model_name],
                'last_updated': datetime.now().isoformat()
            }
            
            production_branch.objects.upload(
                path=monitoring_path,
                data=json.dumps(monitoring_data, indent=2).encode(),
                content_type='application/json'
            )
            
        except Exception as e:
            self.logger.error(f"Error saving monitoring data: {e}")
    
    def _save_drift_detection_results(self, model_name: str, drift_result: Dict[str, Any]):
        """Save drift detection results to lakeFS"""
        
        try:
            production_branch = self.repo.branches.get('production')
            
            drift_path = f"monitoring/{model_name}/drift_detection.json"
            
            # Load existing drift data
            try:
                drift_obj = production_branch.objects.get(drift_path)
                existing_data = json.loads(drift_obj.reader().read().decode())
                existing_data['results'].append(drift_result)
            except:
                existing_data = {
                    'model_name': model_name,
                    'results': [drift_result],
                    'last_updated': datetime.now().isoformat()
                }
            
            production_branch.objects.upload(
                path=drift_path,
                data=json.dumps(existing_data, indent=2).encode(),
                content_type='application/json'
            )
            
        except Exception as e:
            self.logger.error(f"Error saving drift detection results: {e}")
    
    def _send_notification(self, subject: str, message: str, recipients: List[str]):
        """Send email notification"""
        
        try:
            # This is a simplified email notification
            # In practice, you'd configure SMTP settings
            self.logger.info(f"NOTIFICATION: {subject}")
            self.logger.info(f"Recipients: {', '.join(recipients)}")
            self.logger.info(f"Message: {message}")
            
            # Actual email sending would go here
            # msg = MIMEText(message)
            # msg['Subject'] = subject
            # msg['From'] = 'mlops@company.com'
            # msg['To'] = ', '.join(recipients)
            # 
            # with smtplib.SMTP('localhost') as server:
            #     server.send_message(msg)
            
        except Exception as e:
            self.logger.error(f"Error sending notification: {e}")
    
    def run_monitoring_loop(self):
        """Run the continuous monitoring loop"""
        
        self.logger.info("Starting MLOps monitoring loop")
        
        while True:
            try:
                # Run scheduled tasks
                schedule.run_pending()
                
                # Sleep for a minute
                time.sleep(60)
                
            except KeyboardInterrupt:
                self.logger.info("Stopping MLOps monitoring loop")
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(60)  # Continue after error
    
    def generate_mlops_report(self, model_name: str, days: int = 30) -> Dict[str, Any]:
        """Generate MLOps report for a model"""
        
        if model_name not in self.configs:
            raise ValueError(f"Model {model_name} not registered in MLOps pipeline")
        
        # Get monitoring data
        recent_metrics = []
        cutoff_date = datetime.now() - timedelta(days=days)
        
        for metric in self.monitoring_data.get(model_name, []):
            metric_date = datetime.fromisoformat(metric['timestamp'])
            if metric_date >= cutoff_date:
                recent_metrics.append(metric)
        
        # Calculate summary statistics
        if recent_metrics:
            accuracies = [m['accuracy'] for m in recent_metrics]
            avg_accuracy = np.mean(accuracies)
            min_accuracy = np.min(accuracies)
            max_accuracy = np.max(accuracies)
            accuracy_trend = 'improving' if accuracies[-1] > accuracies[0] else 'declining'
        else:
            avg_accuracy = min_accuracy = max_accuracy = 0.0
            accuracy_trend = 'no_data'
        
        # Get model information
        config = self.configs[model_name]
        
        report = {
            'model_name': model_name,
            'report_period_days': days,
            'generated_at': datetime.now().isoformat(),
            'config': config.__dict__,
            'performance_summary': {
                'avg_accuracy': avg_accuracy,
                'min_accuracy': min_accuracy,
                'max_accuracy': max_accuracy,
                'accuracy_trend': accuracy_trend,
                'total_predictions': sum(m['sample_size'] for m in recent_metrics),
                'monitoring_points': len(recent_metrics)
            },
            'recent_metrics': recent_metrics[-10:],  # Last 10 data points
            'alerts_triggered': self._count_recent_alerts(model_name, days),
            'retraining_history': self._get_retraining_history(model_name, days)
        }
        
        return report
    
    def _count_recent_alerts(self, model_name: str, days: int) -> int:
        """Count recent alerts for a model"""
        # This would query alert logs in a real implementation
        return 0
    
    def _get_retraining_history(self, model_name: str, days: int) -> List[Dict[str, Any]]:
        """Get retraining history for a model"""
        # This would query retraining logs in a real implementation
        return []

# Example usage
def setup_mlops_pipeline():
    """Set up a complete MLOps pipeline"""
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize MLOps pipeline
    mlops = MLOpsPipeline()
    
    # Configure model pipeline
    config = MLOpsConfig(
        model_name="customer_churn_predictor",
        dataset_path="datasets/raw/customer_churn_dataset.csv",
        target_column="target",
        feature_columns=['feature_0', 'feature_1', 'feature_2'],
        model_type="random_forest_classifier",
        hyperparameters={'n_estimators': 100, 'random_state': 42},
        retraining_schedule="02:00",  # 2 AM daily
        performance_threshold=0.85,
        data_drift_threshold=0.05,
        alert_recipients=["ml-team@company.com", "ops-team@company.com"],
        auto_deploy=False  # Manual approval required
    )
    
    # Register model for MLOps
    mlops.register_model_pipeline(config)
    
    # Simulate monitoring data
    np.random.seed(42)
    for i in range(10):
        # Simulate predictions and actuals
        predictions = pd.Series(np.random.choice([0, 1], 100))
        actuals = pd.Series(np.random.choice([0, 1], 100))
        
        mlops.monitor_model_performance("customer_churn_predictor", predictions, actuals)
    
    # Generate report
    report = mlops.generate_mlops_report("customer_churn_predictor")
    
    print("MLOps Report:")
    print(f"Model: {report['model_name']}")
    print(f"Average Accuracy: {report['performance_summary']['avg_accuracy']:.4f}")
    print(f"Accuracy Trend: {report['performance_summary']['accuracy_trend']}")
    print(f"Total Predictions: {report['performance_summary']['total_predictions']}")
    
    return mlops

if __name__ == "__main__":
    mlops_pipeline = setup_mlops_pipeline()
    
    # In production, you would run:
    # mlops_pipeline.run_monitoring_loop()
```## 
Best Practices and Production Considerations

### ML Experiment Tracking Best Practices

1. **Comprehensive Logging**
   - Log all hyperparameters, even default values
   - Track data versions and preprocessing steps
   - Save model artifacts and metadata
   - Record environment information (Python version, library versions)

2. **Reproducibility**
   - Set random seeds consistently
   - Version control your code alongside experiments
   - Document data sources and preprocessing steps
   - Use containerization for consistent environments

3. **Experiment Organization**
   - Use meaningful experiment names and descriptions
   - Tag experiments with relevant metadata
   - Group related experiments together
   - Maintain experiment lineage and relationships

4. **Model Versioning**
   - Version models with semantic versioning
   - Track model lineage and experiment relationships
   - Maintain model metadata and signatures
   - Implement proper model lifecycle management

5. **Performance Monitoring**
   - Monitor model performance in production
   - Set up alerts for performance degradation
   - Track data drift and model decay
   - Implement automated retraining pipelines

### Troubleshooting Common Issues

1. **Large Model Storage**
```python
# Use model compression for large models
import joblib

# Save with compression
joblib.dump(model, 'model.joblib', compress=3)

# Or use model-specific serialization
import pickle
with open('model.pkl', 'wb') as f:
    pickle.dump(model, f, protocol=pickle.HIGHEST_PROTOCOL)
```

2. **Memory Issues with Large Datasets**
```python
# Process data in chunks
def train_model_incrementally(model, data_path, chunk_size=1000):
    for chunk in pd.read_csv(data_path, chunksize=chunk_size):
        # Partial fit for models that support it
        if hasattr(model, 'partial_fit'):
            model.partial_fit(chunk.drop('target', axis=1), chunk['target'])
        else:
            # Accumulate data and train in batches
            pass
```

3. **Experiment Comparison Issues**
```python
# Ensure fair comparison
def compare_experiments_fairly(experiments):
    # Use same data splits
    # Use same evaluation metrics
    # Account for randomness with multiple runs
    # Use statistical significance tests
    pass
```

### Integration with External Tools

1. **MLflow Integration**
```python
import mlflow
import mlflow.sklearn

# Log to both lakeFS and MLflow
def log_to_both_systems(model, metrics, artifacts):
    # Log to lakeFS (our implementation)
    tracker.log_model(model)
    tracker.log_metrics(metrics)
    
    # Log to MLflow
    with mlflow.start_run():
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, "model")
```

2. **Weights & Biases Integration**
```python
import wandb

# Initialize both tracking systems
wandb.init(project="ml-experiments")
tracker = MLExperimentTracker()

# Log to both systems
def dual_logging(metrics, artifacts):
    # Log to lakeFS
    tracker.log_metrics(metrics)
    
    # Log to W&B
    wandb.log(metrics)
```

3. **Kubernetes Deployment**
```yaml
# k8s-ml-pipeline.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: ml-experiment
spec:
  template:
    spec:
      containers:
      - name: ml-experiment
        image: ml-experiment:latest
        env:
        - name: LAKEFS_HOST
          value: "http://lakefs-service:8000"
        - name: EXPERIMENT_NAME
          value: "customer-churn-v2"
        command: ["python", "run_experiment.py"]
      restartPolicy: Never
```

## Next Steps

### Advanced Topics to Explore

1. **[Data Science Workflow](data-science-workflow.md)** - Interactive analysis patterns
2. **[ETL Pipeline Tutorial](etl-pipeline.md)** - Production data pipelines
3. **[Advanced Features](../high-level-sdk/advanced.md)** - Performance optimization
4. **[Best Practices](../reference/best-practices.md)** - Production deployment

### Integration Opportunities

1. **AutoML Integration** - Combine with AutoML tools for automated model selection
2. **Feature Stores** - Integrate with feature stores for consistent feature management
3. **Model Serving** - Deploy models with serving frameworks like Seldon or KServe
4. **CI/CD Integration** - Automated model testing and deployment pipelines

### Advanced MLOps Patterns

1. **Multi-Model Pipelines** - Manage ensembles and model chains
2. **Federated Learning** - Distributed model training across multiple data sources
3. **Model Interpretability** - Track and version model explanations
4. **Compliance and Governance** - Audit trails and regulatory compliance

## Troubleshooting

### Common Issues

1. **Experiment Tracking Failures**
```python
# Implement retry logic for tracking operations
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def robust_log_metric(key, value):
    try:
        tracker.log_metric(key, value)
    except Exception as e:
        logger.warning(f"Failed to log metric {key}: {e}")
        raise
```

2. **Model Loading Issues**
```python
# Handle model loading errors gracefully
def safe_load_model(model_path):
    try:
        return joblib.load(model_path)
    except Exception as e:
        logger.error(f"Failed to load model from {model_path}: {e}")
        # Try alternative loading methods
        try:
            import pickle
            with open(model_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e2:
            logger.error(f"Alternative loading also failed: {e2}")
            raise
```

3. **Branch Management Issues**
```python
# Clean up experiment branches periodically
def cleanup_old_experiment_branches(days_old=30):
    cutoff_date = datetime.now() - timedelta(days=days_old)
    
    for branch in repo.branches.list():
        if branch.id.startswith('experiment/'):
            # Check branch age and delete if old
            pass
```

## See Also

**Prerequisites and Setup:**
- **[Python SDK Overview](../index.md)** - Compare all Python SDK options
- **[Getting Started Guide](../getting-started.md)** - Installation and authentication setup
- **[High-Level SDK Quickstart](../high-level-sdk/quickstart.md)** - Basic operations

**Related Tutorials:**
- **[Data Science Workflow](data-science-workflow.md)** - Interactive analysis patterns
- **[ETL Pipeline Tutorial](etl-pipeline.md)** - Production data pipeline patterns

**Advanced Features:**
- **[Transaction Patterns](../high-level-sdk/transactions.md)** - Atomic operations
- **[lakefs-spec Integration](../lakefs-spec/integrations.md)** - Filesystem operations
- **[Best Practices](../reference/best-practices.md)** - Production deployment guidance

**External Resources:**
- **[MLflow Documentation](https://mlflow.org/docs/latest/index.html){:target="_blank"}** - ML lifecycle management
- **[Weights & Biases](https://docs.wandb.ai/){:target="_blank"}** - Experiment tracking and visualization
- **[Kubeflow](https://www.kubeflow.org/docs/){:target="_blank"}** - ML workflows on Kubernetes
- **[DVC Documentation](https://dvc.org/doc){:target="_blank"}** - Data version control
- **[Great Expectations](https://greatexpectations.io/){:target="_blank"}** - Data validation and profiling