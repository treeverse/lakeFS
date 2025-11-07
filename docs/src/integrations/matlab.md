# MATLAB Integration

Integrate lakeFS with MATLAB for data version control in MATLAB data science, data engineering, and machine learning workflows.

## Overview

The lakeFS MATLAB integration provides native MATLAB functions to interact with lakeFS repositories, enabling:

- **Version control for data**: Track datasets, models, and results across experiments
- **Reproducible research**: Link code to specific data versions with commit IDs
- **Isolated experimentation**: Create branches for parallel experiments without data duplication
- **Direct data access**: Mount lakeFS repositories as local directories using Everest

This integration uses two MATLAB helper classes:
- **`lakefs.m`**: Core lakeFS operations (branches, commits, tags, metadata)
- **everest.m**: File system mounting for direct data access

## Prerequisites

Before using lakeFS with MATLAB, ensure you have:

1. **lakeFS Server**: Running instance (local or cloud)
   - [Quick Start Guide](https://docs.lakefs.io/latest/quickstart/)
   - [Cloud Setup](https://lakefs.cloud)

2. **lakectl CLI**: Installed and configured
   - [lakectl Installation](https://docs.lakefs.io/latest/reference/cli/)
   - Configuration file at `~/.lakectl.yaml`

3. **Everest** (optional, for mounting): File system interface
   - [Everest Documentation](https://docs.lakefs.io/latest/reference/mount/)
   - Install: `brew install treeverse/brew/everest` (macOS)

4. **MATLAB**: Version R2023a or later recommended


## Installation

### Download Helper Files

Download the MATLAB helper classes:
- [`lakefs.m`](https://github.com/treeverse/lakeFS-samples/matlab/lakefs.m) - Core lakeFS operations
- [`everest.m`](https://github.com/treeverse/lakeFS-samples/matlab/everest.m) - File system mounting

Place both files in your MATLAB project directory or add to your MATLAB path:

```matlab
% Add helpers to MATLAB path
addpath('/path/to/helpers');
```

### Configure lakeFS Connection

Create your lakeFS configuration by copying and editing the template:

**`lakefs_template.m`**:
```matlab
classdef lakefs_template
    % LAKEFS_TEMPLATE - Configuration template for lakeFS connection
    %
    % SETUP:
    % 1. Copy this file to lakefs.m in your project directory
    % 2. Fill in your credentials below
    % 3. Keep lakefs.m out of version control (.gitignore)
    
    properties(Constant)
        % === LOCAL CONFIGURATION ===
        LOCAL_ENDPOINT = 'http://localhost:8000'
        LOCAL_ACCESS_KEY = 'YOUR_ACCESS_KEY_HERE'
        LOCAL_SECRET_KEY = 'YOUR_SECRET_KEY_HERE'
        
        % === CLOUD CONFIGURATION ===
        CLOUD_ENDPOINT = 'https://your-org.us-east-1.lakefscloud.io'
        CLOUD_ACCESS_KEY = 'YOUR_CLOUD_ACCESS_KEY_HERE'
        CLOUD_SECRET_KEY = 'YOUR_CLOUD_SECRET_KEY_HERE'
        
        % === CURRENT CONFIGURATION (will be set by configure()) ===
        % These will be populated when you call lakefs.configure()
    end
    
    properties(Access=private)
        current_endpoint
        current_access_key
        current_secret_key
    end
    
    methods(Static)
        function configure(environment)
            % CONFIGURE - Set active lakeFS environment
            % 
            % Syntax:
            %   lakefs.configure('local')   % Use local lakeFS
            %   lakefs.configure('cloud')   % Use cloud lakeFS
        end
    end
end
```

**Configuration steps**:

1. Copy `lakefs_template.m` to `lakefs.m`
2. Edit credentials in `lakefs.m`
3. Add `lakefs.m` to `.gitignore` to keep credentials private
4. Set active environment in your scripts:

```matlab
% Use local lakeFS instance
lakefs.configure('local');

% Or use cloud lakeFS instance
lakefs.configure('cloud');
```

## Basic Usage

### Repository Operations

**List branches**:
```matlab
% List all branches in a repository
branches = lakefs.list_branches('my-repo');
disp(branches.branch);
```

**List tags**:
```matlab
% List all tags
tags = lakefs.list_tags('my-repo');
disp(tags.tag);
```

**View commit history**:
```matlab
% Get commit log for a branch
commits = lakefs.log('my-repo', 'main', 'amount', 5);
for i = 1:height(commits)
    fprintf('%s: %s\n', commits.id{i}(1:12), commits.message{i});
end
```

### Branch Management

**Create a branch**:
```matlab
% Create experiment branch from main
lakefs.create_branch('my-repo', 'experiment-1', 'main');
```

**Compare branches**:
```matlab
% Show differences between branches
diff = lakefs.diff('my-repo', 'main', 'experiment-1');
fprintf('Changed objects: %d\n', height(diff));
```

### Data Operations

**Upload data**:
```matlab
% Upload local file to lakeFS branch
lakefs.upload('my-repo', 'experiment-1', ...
    'data/results.mat', ...      % Destination in lakeFS
    'local/output/results.mat');  % Local source file
```

**Download data**:
```matlab
% Download file from lakeFS
lakefs.download('my-repo', 'main', ...
    'models/trained_model.mat', ...  % Source in lakeFS
    'local/models/model.mat');        % Local destination
```

**Commit changes**:
```matlab
% Commit with metadata
metadata = struct();
metadata.accuracy = '0.945';
metadata.training_time = '120.5';
metadata.dataset = 'v1.2';

lakefs.commit('my-repo', 'experiment-1', ...
    'Trained model with improved accuracy', ...
    'metadata', metadata);
```

### Mounting (Everest)

For direct file system access, use Everest to mount lakeFS paths:

```matlab
% Mount a branch
everest.mount('lakefs://my-repo/main/', 'local_data/');

% Read files directly
data = load('local_data/results.mat');

% Unmount when done
everest.umount('local_data/');
```

**Note**: Mounting enables standard MATLAB file I/O functions to work with lakeFS data without explicit upload/download operations.

## Core API Reference

### lakefs.m - Repository Operations

#### Configuration

```matlab
lakefs.configure(environment)
```
**Description**: Set active lakeFS environment  
**Parameters**: 
- `environment` (string): `'local'` or `'cloud'`

---

#### Branch Operations

```matlab
branches = lakefs.list_branches(repo)
```
**Description**: List all branches in repository  
**Parameters**: 
- `repo` (string): Repository name  
**Returns**: Table with branch names and commit IDs

```matlab
lakefs.create_branch(repo, branch_name, source_ref)
```
**Description**: Create new branch from source reference  
**Parameters**: 
- `repo` (string): Repository name
- `branch_name` (string): New branch name
- `source_ref` (string): Source branch or commit ID

---

#### Commit Operations

```matlab
commits = lakefs.log(repo, ref, varargin)
```
**Description**: Get commit history  
**Parameters**: 
- `repo` (string): Repository name
- `ref` (string): Branch or commit reference
- Optional: `'amount', N` - Limit number of commits returned  
**Returns**: Table with commit IDs, messages, timestamps, metadata

```matlab
lakefs.commit(repo, branch, message, varargin)
```
**Description**: Commit changes to branch  
**Parameters**: 
- `repo` (string): Repository name
- `branch` (string): Branch name
- `message` (string): Commit message
- Optional: `'metadata', struct` - Attach metadata to commit

---

#### Data Transfer

```matlab
lakefs.upload(repo, branch, lakefs_path, local_path)
```
**Description**: Upload local file to lakeFS  
**Parameters**: 
- `repo` (string): Repository name
- `branch` (string): Branch name
- `lakefs_path` (string): Destination path in lakeFS
- `local_path` (string): Source file on local system

```matlab
lakefs.download(repo, ref, lakefs_path, local_path)
```
**Description**: Download file from lakeFS  
**Parameters**: 
- `repo` (string): Repository name
- `ref` (string): Branch, tag, or commit ID
- `lakefs_path` (string): Source path in lakeFS
- `local_path` (string): Destination on local system

---

#### Comparison

```matlab
diff = lakefs.diff(repo, left_ref, right_ref)
```
**Description**: Compare two references  
**Parameters**: 
- `repo` (string): Repository name
- `left_ref` (string): First reference (branch/tag/commit)
- `right_ref` (string): Second reference  
**Returns**: Table showing changed, added, removed objects

---

#### Tags

```matlab
tags = lakefs.list_tags(repo)
```
**Description**: List all tags  
**Parameters**: 
- `repo` (string): Repository name  
**Returns**: Table with tag names and commit IDs

### everest.m - File System Operations

#### Mounting

```matlab
everest.mount(lakefs_uri, mount_dir, varargin)
```
**Description**: Mount lakeFS path as local directory  
**Parameters**: 
- `lakefs_uri` (string): Full lakeFS URI (e.g., `'lakefs://repo/branch/path/'`)
- `mount_dir` (string): Local mount point
- Optional: `'presign', true` - Use presigned URLs (default: true)

```matlab
everest.umount(mount_dir)
```
**Description**: Unmount directory  
**Parameters**: 
- `mount_dir` (string): Local mount point to unmount

```matlab
mounts = everest.list_mounts()
```
**Description**: Show active mounts  
**Returns**: Cell array of currently mounted directories

```matlab
everest.verify()
```
**Description**: Check Everest installation and accessibility

## Example Workflows

### Experiment Tracking

```matlab
% Configure environment
lakefs.configure('local');

% Create experiment branch
lakefs.create_branch('ml-project', 'experiment-3', 'main');

% Train model (your code here)
model = train_model(data);
save('outputs/model_v3.mat', 'model');

% Upload results
lakefs.upload('ml-project', 'experiment-3', ...
    'models/model_v3.mat', 'outputs/model_v3.mat');

% Commit with metrics
metadata = struct();
metadata.accuracy = num2str(model.accuracy);
metadata.loss = num2str(model.loss);
metadata.epochs = num2str(model.epochs);

lakefs.commit('ml-project', 'experiment-3', ...
    'Experiment 3: Increased learning rate', ...
    'metadata', metadata);

% View experiment history
commits = lakefs.log('ml-project', 'experiment-3', 'amount', 1);
fprintf('Experiment accuracy: %s\n', commits.metadata{1}.accuracy);
```

### Data Version Comparison

```matlab
% Compare two data versions
diff = lakefs.diff('research-data', 'v1.0', 'v2.0');

% Show what changed
fprintf('Data changes between versions:\n');
for i = 1:height(diff)
    fprintf('  %s: %s\n', diff.type{i}, diff.path{i});
end

% Download specific version
lakefs.download('research-data', 'v1.0', ...
    'datasets/training_data.mat', 'data/v1_training.mat');

lakefs.download('research-data', 'v2.0', ...
    'datasets/training_data.mat', 'data/v2_training.mat');

% Compare in MATLAB
v1 = load('data/v1_training.mat');
v2 = load('data/v2_training.mat');
fprintf('V1 samples: %d\n', size(v1.data, 1));
fprintf('V2 samples: %d\n', size(v2.data, 1));
```

### Reproducible Analysis

```matlab
% Record exact data version used
commits = lakefs.log('sensor-data', 'main', 'amount', 1);
data_commit = commits.id{1};

% Mount data
everest.mount('lakefs://sensor-data/main/', 'analysis_data/');

% Run analysis
data = load('analysis_data/sensor_readings.mat');
results = analyze_sensors(data);

% Unmount
everest.umount('analysis_data/');

% Store results with data lineage
lakefs.upload('sensor-data', 'analysis-results', ...
    'results/analysis_output.mat', 'results/output.mat');

metadata = struct();
metadata.data_commit = data_commit;  % Track source data version
metadata.analysis_date = datestr(now);
metadata.mean_value = num2str(mean(results.values));

lakefs.commit('sensor-data', 'analysis-results', ...
    'Sensor analysis results', 'metadata', metadata);

% Later: Reproduce exact analysis
fprintf('Analysis used data from commit: %s\n', data_commit);
```

## Advanced Topics

### Environment Switching

Switch between local and cloud environments in the same session:

```matlab
% Work with local instance
lakefs.configure('local');
local_branches = lakefs.list_branches('dev-repo');

% Switch to cloud
lakefs.configure('cloud');
cloud_branches = lakefs.list_branches('prod-repo');
```

### Metadata Best Practices

Structure metadata for maximum value:

```matlab
metadata = struct();

% Experiment details
metadata.experiment_id = 'exp-2024-001';
metadata.hypothesis = 'increased_batch_size';

% Quantitative results
metadata.accuracy = sprintf('%.4f', results.accuracy);
metadata.training_time_sec = num2str(results.time);

% Computational environment
metadata.matlab_version = version;
metadata.gpu_used = 'NVIDIA RTX 4090';

% Data lineage
metadata.training_data_commit = training_commit_id;
metadata.validation_data_commit = val_commit_id;

lakefs.commit(repo, branch, message, 'metadata', metadata);
```

### Working with Large Datasets

For large datasets, prefer mounting over upload/download:

```matlab
% Mount for direct access (no copying)
everest.mount('lakefs://large-data/main/datasets/', 'data/');

% Process data in chunks
datastore = imageDatastore('data/images', ...
    IncludeSubfolders=true, ...
    LabelSource="foldernames");

% Process without loading all into memory
while hasdata(datastore)
    img = read(datastore);
    process_image(img);
end

% Unmount when done
everest.umount('data/');
```

## Troubleshooting

### Connection Issues

**Error: Cannot connect to lakeFS**

Verify configuration:
```matlab
% Check lakectl is configured
!lakectl repo list

% Test lakefs.m configuration
lakefs.configure('local');  % or 'cloud'
```

### Mount Issues

**Error: Mount failed**

Verify Everest installation:
```matlab
everest.verify();

% Check if Everest is in PATH
!which everest
```

**Mount not showing files**

Wait for mount to complete:
```matlab
everest.mount('lakefs://repo/branch/', 'data/');
pause(2);  % Give mount time to initialize
dir('data/')  % Should now show files
```

### Path Issues

**Error: File not found in lakeFS**

Verify path format:
```matlab
% Correct: Relative path from branch root
lakefs.upload('repo', 'branch', 'data/file.mat', 'local.mat');

% Incorrect: Leading slash
lakefs.upload('repo', 'branch', '/data/file.mat', 'local.mat');
```

## Additional Resources

- [lakeFS Documentation](https://docs.lakefs.io/)
- [lakectl CLI Reference](https://docs.lakefs.io/latest/reference/cli/)
- [Everest Mount Reference](https://docs.lakefs.io/latest/reference/mount/)
- [lakeFS Samples Repository](https://github.com/treeverse/lakeFS-samples)
- [lakeFS Community Slack](https://lakefs.io/slack)

## Version Compatibility

| Component | Minimum Version | Recommended |
|-----------|----------------|-------------|
| MATLAB | R2020a | R2023a+ |
| lakeFS | 0.100.0 | Latest |
| lakectl | 0.100.0 | Latest |
| Everest | 0.1.0 | Latest |

**Note**: MATLAB R2023a introduced `name=value` syntax for function arguments. Earlier versions use `'name', value` pairs. The helper files are compatible with both syntaxes.
