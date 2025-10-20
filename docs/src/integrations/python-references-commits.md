---
title: Python - References & Commits
description: Navigate commit history, understand references, and track data lineage in lakeFS with Python
---

# Working with References & Commits

References, commits, and commit metadata are fundamental to understanding and auditing changes in lakeFS. This guide covers navigating commit history, working with references, and using metadata for tracking and lineage.

## Prerequisites

- lakeFS server running and accessible
- Python SDK installed: `pip install lakefs`
- A repository with commit history (or create one in the examples)
- Proper credentials configured

## Understanding References

### What are References?

A reference is any pointer to a commit in lakeFS:

- **Branch**: A mutable reference (changes as new commits are made)
- **Tag**: An immutable reference (always points to the same commit)
- **Commit ID**: A specific commit's unique identifier
- **Ref Expression**: Advanced reference syntax like `main~2` (2 commits before main)

### Creating References

Get reference objects for any valid reference:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

# Reference to branch head
main_ref = repo.ref("main")
print(f"Main reference: {main_ref.id}")

# Reference to specific commit
commit_ref = repo.ref("abc123def456")
print(f"Commit reference: {commit_ref.id}")

# Reference to tag
tag_ref = repo.ref("v1.0.0")
print(f"Tag reference: {tag_ref.id}")

# Advanced reference expressions
two_back = repo.ref("main~2")  # Two commits before main
print(f"Two commits back: {two_back.id}")
```

### Getting Commit Information from References

```python
import lakefs

repo = lakefs.repository("my-data-repo")
ref = repo.ref("main")

# Get the underlying commit
commit = ref.get_commit()

print(f"Commit ID: {commit.id}")
print(f"Message: {commit.message}")
print(f"Committer: {commit.committer}")
print(f"Created: {commit.creation_date}")
print(f"Parents: {commit.parents}")
print(f"Metadata: {commit.metadata}")
```

## Understanding Commits

### What are Commits?

Commits create immutable snapshots of changes on a branch. Each commit has a unique ID and optional metadata:

```python
branch = lakefs.repository("my-repo").branch("main")

# Create a commit
ref = branch.commit(
    message="Add new dataset",
    metadata={"author": "data-team", "version": "1.0"}
)
print(f"Committed: {ref.id}")
```

Commits are the fundamental building blocks of version control in lakeFS. They allow you to:

- **Track changes** over time with unique identifiers
- **Capture metadata** for auditing and tracking
- **Create reproducible snapshots** for data lineage
- **Understand who made changes** and when

## Working with Commits

### Getting Commit Details

Retrieve detailed information about a specific commit:

```python
import lakefs
from datetime import datetime

repo = lakefs.repository("my-data-repo")

try:
    # Get commit by ID
    commit_ref = repo.commit("abc123def456xyz")
    commit = commit_ref.get_commit()
    
    print(f"Commit Details:")
    print(f"  ID: {commit.id}")
    print(f"  Message: {commit.message}")
    print(f"  Committer: {commit.committer}")
    print(f"  Timestamp: {datetime.fromtimestamp(commit.creation_date)}")
    print(f"  Parents: {', '.join(commit.parents) if commit.parents else 'None'}")
    
    # Check for merge commit
    if len(commit.parents) > 1:
        print(f"  Type: Merge commit (from {len(commit.parents)} parents)")
    else:
        print(f"  Type: Regular commit")
    
except Exception as e:
    print(f"Commit not found: {e}")
```

### Accessing Commit Metadata

Retrieve custom metadata attached to commits:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Get the latest commit
commit = branch.get_commit()

print(f"Commit: {commit.id[:8]}")
print(f"Message: {commit.message}")

if commit.metadata:
    print("Metadata:")
    for key, value in commit.metadata.items():
        print(f"  {key}: {value}")
else:
    print("No metadata")
```

### Creating Commits with Metadata

Create commits with custom metadata for tracking:

```python
import lakefs
import json
from datetime import datetime

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Upload data
branch.object("data/dataset.csv").upload(data=b"id,value\n1,100\n2,200")

# Commit with rich metadata
commit_ref = branch.commit(
    message="Add customer dataset v2",
    metadata={
        "author": "data-team",
        "version": "2.0",
        "dataset-type": "raw",
        "source": "database-export",
        "record-count": "10000",
        "timestamp": datetime.now().isoformat(),
        "data-owner": "analytics-team@company.com"
    }
)

print(f"Committed: {commit_ref.id}")
print(f"Metadata stored for tracking")
```

## Navigating Commit History

### List Commits (Log)

View the commit history of a branch:

```python
import lakefs
from datetime import datetime

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

print("Recent commits:")
for i, commit in enumerate(branch.log(max_amount=10)):
    timestamp = datetime.fromtimestamp(commit.creation_date)
    print(f"  {i+1}. {commit.id[:8]} - {commit.message[:40]} ({timestamp})")
```

### Get Commits with Filtering

Retrieve specific commits from history:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

# Get commits and filter by author
all_commits = list(branch.log(max_amount=100))

data_team_commits = [
    c for c in all_commits 
    if c.metadata and c.metadata.get("author") == "data-team"
]

print(f"Data team commits: {len(data_team_commits)}")
for commit in data_team_commits[:5]:
    print(f"  {commit.id[:8]} - {commit.message}")
```

### Track Commits by Metadata

Find commits based on custom metadata:

```python
import lakefs

def find_commits_by_metadata(repo_name, branch_name, key, value):
    """Find commits with specific metadata"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)
    
    matching_commits = []
    
    for commit in branch.log(max_amount=1000):
        if commit.metadata and commit.metadata.get(key) == value:
            matching_commits.append(commit)
    
    return matching_commits


# Usage:
commits = find_commits_by_metadata("analytics-repo", "main", "dataset-type", "clean")
print(f"Found {len(commits)} commits with dataset-type=clean")

for commit in commits[:5]:
    print(f"  {commit.id[:8]} - {commit.message}")
```

## Comparing References (Diffs)

### Diff Between Two References

See what changed between any two references:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
main = repo.ref("main")
dev = repo.ref("develop")

print("Changes from main to develop:")
for change in main.diff(other_ref=dev):
    print(f"  {change.type:10} {change.path} ({change.size_bytes} bytes)")

# Count changes
changes = list(main.diff(other_ref=dev))
print(f"\nTotal changes: {len(changes)}")
```

### Diff with Filtering

Filter diff results by path or change type:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
tag_v1 = repo.ref("v1.0.0")
tag_v2 = repo.ref("v2.0.0")

# Get all changes
all_changes = list(tag_v1.diff(other_ref=tag_v2))

# Filter by change type
added = [c for c in all_changes if c.type == "added"]
removed = [c for c in all_changes if c.type == "removed"]
modified = [c for c in all_changes if c.type == "modified"]

print(f"Added: {len(added)}")
print(f"Removed: {len(removed)}")
print(f"Modified: {len(modified)}")

# Filter by path prefix
data_changes = [c for c in all_changes if c.path.startswith("data/")]
print(f"Changes in data/ folder: {len(data_changes)}")
```

### Detailed Diff with Size Analysis

Analyze what changed with size information:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
ref1 = repo.ref("commit1")
ref2 = repo.ref("commit2")

print("Detailed changes:")
for change in ref1.diff(other_ref=ref2):
    size_info = f" ({change.size_bytes} bytes)" if change.size_bytes else ""
    print(f"  {change.type:10} {change.path}{size_info}")
```

## Tracking Lineage and Audit Trails

### Create a Lineage Trail

Track data through multiple transformations with metadata:

```python
import lakefs
import json
from datetime import datetime

def record_lineage_stage(repo_name, stage_name, description):
    """Record a stage in data lineage"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch("main")
    
    # Create a lineage marker file
    lineage_entry = {
        "stage": stage_name,
        "description": description,
        "timestamp": datetime.now().isoformat(),
        "commit": ""  # Will be filled after commit
    }
    
    # Write lineage entry
    lineage_path = f"lineage/{stage_name}.json"
    branch.object(lineage_path).upload(
        data=json.dumps(lineage_entry).encode()
    )
    
    # Commit with stage metadata
    ref = branch.commit(
        message=f"Lineage: {stage_name}",
        metadata={
            "lineage-stage": stage_name,
            "lineage-description": description,
            "lineage-timestamp": lineage_entry["timestamp"]
        }
    )
    
    print(f"Recorded lineage stage: {stage_name}")
    print(f"  Commit: {ref.id}")
    
    return ref


# Usage:
lineage_stages = [
    ("raw-ingest", "Ingested raw customer data"),
    ("data-cleaning", "Cleaned and validated data"),
    ("feature-engineering", "Created features for ML"),
    ("model-ready", "Final dataset ready for training")
]

for stage, desc in lineage_stages:
    record_lineage_stage("ml-repo", stage, desc)
```

### Query Lineage History

Retrieve the lineage trail:

```python
import lakefs

def get_data_lineage(repo_name, branch_name):
    """Get the lineage trail for data"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)
    
    lineage = []
    
    for commit in branch.log(max_amount=1000):
        if commit.metadata and "lineage-stage" in commit.metadata:
            lineage.append({
                "commit": commit.id[:8],
                "stage": commit.metadata.get("lineage-stage"),
                "description": commit.metadata.get("lineage-description"),
                "timestamp": commit.metadata.get("lineage-timestamp")
            })
    
    return lineage


# Usage:
lineage = get_data_lineage("ml-repo", "main")
print("Data Lineage Trail:")
for entry in reversed(lineage):  # Show in chronological order
    print(f"  {entry['stage']:20} - {entry['description']}")
```

### Audit Trail

Create a complete audit trail for compliance:

```python
import lakefs
from datetime import datetime

def generate_audit_report(repo_name, branch_name, days_back=30):
    """Generate audit report for a branch"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)
    
    current_time = datetime.now().timestamp()
    cutoff_time = current_time - (days_back * 24 * 60 * 60)
    
    audit_entries = []
    
    for commit in branch.log(max_amount=10000):
        if commit.creation_date < cutoff_time:
            break
        
        audit_entries.append({
            "timestamp": datetime.fromtimestamp(commit.creation_date),
            "commit_id": commit.id[:8],
            "message": commit.message,
            "committer": commit.committer,
            "parents": len(commit.parents),
            "metadata": commit.metadata or {}
        })
    
    return audit_entries


# Usage:
audit_report = generate_audit_report("sensitive-repo", "main", days_back=30)
print("Audit Report (Last 30 Days):")
print(f"Total changes: {len(audit_report)}")
print("\nRecent activity:")

for entry in audit_report[:10]:
    print(f"  {entry['timestamp']} - {entry['committer']}: {entry['message'][:50]}")
```

## Commit Relationships

### Identify Merge Commits

Find and analyze merge commits:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("main")

print("Merge commits:")
for i, commit in enumerate(branch.log(max_amount=50)):
    if len(commit.parents) > 1:
        print(f"  {commit.id[:8]} - Merged {len(commit.parents)} branches")
        print(f"    Message: {commit.message}")
        print(f"    Parents: {', '.join([p[:8] for p in commit.parents])}")
```

### Trace Commit Ancestry

Follow a commit back through its parents:

```python
import lakefs

def trace_ancestry(repo_name, commit_id, depth=5):
    """Trace commit ancestry up to specified depth"""
    repo = lakefs.repository(repo_name)
    ancestry = []
    
    current_id = commit_id
    
    for level in range(depth):
        try:
            commit_ref = repo.commit(current_id)
            commit = commit_ref.get_commit()
            
            ancestry.append({
                "level": level,
                "commit_id": commit.id[:8],
                "message": commit.message,
                "parents": commit.parents
            })
            
            # Move to first parent
            if commit.parents:
                current_id = commit.parents[0]
            else:
                break
                
        except Exception as e:
            print(f"Error at level {level}: {e}")
            break
    
    return ancestry


# Usage:
ancestry = trace_ancestry("my-repo", "abc123def456", depth=5)
print("Commit Ancestry:")
for entry in ancestry:
    indent = "  " * entry["level"]
    print(f"{indent}└─ {entry['commit_id']} - {entry['message']}")
```

## Real-World Workflows

### Data Quality Change Tracking

Track when and why data quality rules changed:

```python
import lakefs
import json

def track_quality_changes(repo_name):
    """Find all commits that changed quality rules"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch("main")
    
    quality_changes = []
    
    for commit in branch.log(max_amount=1000):
        # Check if commit mentions quality rules
        if "quality" in commit.message.lower() or "validation" in commit.message.lower():
            quality_changes.append({
                "commit": commit.id[:8],
                "message": commit.message,
                "author": commit.metadata.get("author") if commit.metadata else "unknown",
                "date": commit.creation_date
            })
    
    return quality_changes


# Usage:
changes = track_quality_changes("analytics-repo")
print("Quality Rule Changes:")
for change in changes:
    print(f"  {change['commit']} - {change['message']} (by {change['author']})")
```

### Finding When Data Changed

Locate the exact commit that changed a specific object:

```python
import lakefs

def find_last_change_to_object(repo_name, branch_name, object_path):
    """Find the most recent commit that modified an object"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch(branch_name)
    
    for commit in branch.log(max_amount=10000):
        try:
            # Try to get this object at this commit
            commit_ref = repo.commit(commit.id)
            obj = commit_ref.object(object_path)
            # If we get here, object exists at this commit
            
            # Check if it's different from the current version
            current_obj = branch.object(object_path)
            
            return {
                "last_modified": commit,
                "modified_date": commit.creation_date,
                "committer": commit.committer
            }
            
        except:
            # Object doesn't exist at this point in history
            continue
    
    return None


# Usage:
change = find_last_change_to_object("data-repo", "main", "data/customer_master.csv")
if change:
    print(f"Last modified: {change['last_modified'].message}")
    print(f"By: {change['committer']}")
```

### Tracking Data Origin

Record where data came from:

```python
import lakefs
import json
from datetime import datetime

def record_data_source(repo_name, data_path, source_info):
    """Record the source of imported data"""
    repo = lakefs.repository(repo_name)
    branch = repo.branch("main")
    
    # Create source metadata file
    source_metadata = {
        "data_path": data_path,
        "source": source_info["source"],
        "import_date": datetime.now().isoformat(),
        "record_count": source_info.get("record_count"),
        "import_method": source_info.get("method", "unknown"),
        "validated": source_info.get("validated", False)
    }
    
    # Store metadata
    metadata_path = f".metadata/{data_path}.json"
    branch.object(metadata_path).upload(
        data=json.dumps(source_metadata, indent=2).encode()
    )
    
    # Commit with tracking
    ref = branch.commit(
        message=f"Import: {data_path} from {source_info['source']}",
        metadata={
            "import-source": source_info["source"],
            "import-type": "data-import",
            "data-path": data_path
        }
    )
    
    print(f"Recorded data source: {data_path}")
    print(f"  Source: {source_info['source']}")
    print(f"  Commit: {ref.id[:8]}")
    
    return ref


# Usage:
ref = record_data_source(
    "analytics-repo",
    "customer_data/Q4_2024.csv",
    {
        "source": "CRM Export",
        "record_count": 50000,
        "method": "API",
        "validated": True
    }
)
```

## Error Handling

### Handling Common Reference Errors

```python
import lakefs
from lakefs.exceptions import NotFoundException

repo = lakefs.repository("my-data-repo")

# Reference doesn't exist
try:
    ref = repo.ref("non-existent-ref")
    commit = ref.get_commit()
except NotFoundException:
    print("Reference not found")

# Commit doesn't exist
try:
    commit_ref = repo.commit("nonexistent123")
    commit = commit_ref.get_commit()
except NotFoundException:
    print("Commit not found")

# Invalid reference expression
try:
    ref = repo.ref("main~1000")  # Try to get 1000 commits back
    commit = ref.get_commit()
except NotFoundException:
    print("Reference expression invalid or out of range")
```

## Best Practices

- **Use meaningful commit messages**: Include context about what changed and why
- **Add metadata to commits**: Include author, type, source, and business context
- **Commit frequently**: Small, logical commits are easier to track and audit
- **Document lineage**: Record data transformation stages for compliance
- **Maintain audit trails**: Log all significant changes for regulatory compliance
- **Use consistent metadata schema**: Standardize metadata keys across your organization
- **Regular cleanup**: Archive old commits if needed for storage management
- **Automated tracking**: Use scripts to record metadata for automated processes
