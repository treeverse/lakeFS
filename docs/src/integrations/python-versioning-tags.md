---
title: Python - Tags & Snapshots
description: Create and manage immutable snapshots using tags in lakeFS with Python
---

# Working with Tags & Snapshots

Tags are immutable pointers to specific commits in lakeFS, making them perfect for marking releases, data versions, and important snapshots. This guide covers all tag operations and common tagging strategies.

## Prerequisites

- lakeFS server running and accessible
- Python SDK installed: `pip install lakefs`
- A repository with commits (or create one in the examples)
- Proper credentials configured

## Understanding Tags

### What are Tags?

Tags mark specific commits as important (e.g., releases):

```python
import lakefs

tag = lakefs.repository("my-repo").tag("v1.0.0").create(
    source_ref="main"
)
```

Tags are immutable pointers to commits that allow you to:

- **Mark releases** for versioning and distribution
- **Create snapshots** for reproducibility and archival
- **Reference important points** in your data history
- **Track data lineage** across versions

Unlike branches, tags never change once created, making them perfect for stable reference points.

## Creating Tags

### Create a Simple Tag

Create a tag pointing to the current head of a branch:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

# Create a tag from the main branch's head
tag = repo.tag("v1.0.0").create(source_ref="main")

print(f"Created tag: v1.0.0")
print(f"Points to commit: {tag.get_commit().id}")
```

### Create a Tag from a Specific Commit

Create a tag pointing to any commit:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
main = repo.branch("main")

# Get a specific commit from history
commits = list(main.log(max_amount=10))

if commits:
    # Tag an older commit
    commit_to_tag = commits[0]  # Most recent
    tag = repo.tag("v1.0.0-rc1").create(source_ref=commit_to_tag.id)
    
    print(f"Tagged commit: {commit_to_tag.id[:8]}")
    print(f"Tag name: v1.0.0-rc1")
```

### Create a Tag from Another Tag

Create a tag based on an existing tag:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

try:
    # Create a new tag from an existing tag
    existing_tag = repo.tag("v1.0.0")
    new_tag = repo.tag("stable").create(source_ref=existing_tag)
    
    print(f"New tag 'stable' points to same commit as 'v1.0.0'")
    
except Exception as e:
    print(f"Error: {e}")
```

### Conditional Tag Creation

Create a tag only if it doesn't already exist:

```python
import lakefs
from lakefs.exceptions import ConflictException

repo = lakefs.repository("my-data-repo")
tag_name = "v2.0.0"

try:
    # Create tag with exist_ok=False (will fail if exists)
    tag = repo.tag(tag_name).create(source_ref="main", exist_ok=False)
    print(f"Created new tag: {tag_name}")
    
except ConflictException:
    print(f"Tag already exists: {tag_name}")
    tag = repo.tag(tag_name)
    print(f"Using existing tag: {tag.get_commit().id}")
```

## Listing and Querying Tags

### List All Tags

List all tags in a repository:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

print("All tags in repository:")
for tag in repo.tags():
    commit = tag.get_commit()
    print(f"  {tag.id:20} -> {commit.id[:8]}... ({commit.message})")
```

### List Tags with Filtering

Filter tags by prefix or limit results:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

# Get all release tags
print("Release tags (v*.*.*):")
release_tags = [t for t in repo.tags() if t.id.startswith("v")]
for tag in release_tags:
    print(f"  {tag.id}")

# Get recent tags
print("\nRecent tags (limit 5):")
recent_tags = []
for i, tag in enumerate(repo.tags()):
    if i >= 5:
        break
    recent_tags.append(tag)
    print(f"  {tag.id}")
```

### Get Tag Information

Get detailed information about a specific tag:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
tag = repo.tag("v1.0.0")

try:
    commit = tag.get_commit()
    
    print(f"Tag: {tag.id}")
    print(f"Commit ID: {commit.id}")
    print(f"Message: {commit.message}")
    print(f"Committer: {commit.committer}")
    print(f"Created: {commit.creation_date}")
    print(f"Metadata: {commit.metadata}")
    
except Exception as e:
    print(f"Tag not found: {e}")
```

## Accessing Data from Tags

### List Objects in a Tagged Version

List all objects in a specific tagged version:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
tag_ref = repo.ref("v1.0.0")  # Use ref() for tag access

# List all objects in this tag
print(f"Objects in v1.0.0:")
for obj in tag_ref.objects():
    print(f"  {obj.path}")

# List specific prefix
print(f"\nModels in v1.0.0:")
for obj in tag_ref.objects(prefix="models/"):
    if hasattr(obj, 'path'):  # It's a file, not a folder
        print(f"  {obj.path} ({obj.size_bytes} bytes)")
```

### Read Data from Tagged Version

Read object contents from a specific tag:

```python
import lakefs
import csv
import io

repo = lakefs.repository("my-data-repo")
tag_ref = repo.ref("v1.0.0")

# Read a CSV file from the tag
try:
    obj = tag_ref.object("data/dataset.csv")
    
    with obj.reader(mode='r') as f:
        reader = csv.reader(f)
        headers = next(reader)
        print(f"Headers: {headers}")
        
        for row in reader:
            print(f"  {row}")
            
except Exception as e:
    print(f"Error reading file: {e}")
```

### Compare Data Across Versions

Compare what changed between two tagged versions:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
tag_v1 = repo.ref("v1.0.0")
tag_v2 = repo.ref("v2.0.0")

# See what changed
print("Changes from v1.0.0 to v2.0.0:")
for change in tag_v1.diff(other_ref=tag_v2):
    print(f"  {change.type:10} {change.path}")

# Count change types
changes = list(tag_v1.diff(other_ref=tag_v2))
added = len([c for c in changes if c.type == "added"])
removed = len([c for c in changes if c.type == "removed"])
modified = len([c for c in changes if c.type == "modified"])

print(f"\nSummary: +{added} -{removed} ~{modified}")
```

## Deleting Tags

### Delete a Single Tag

Remove a tag that's no longer needed:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

try:
    tag = repo.tag("old-release")
    tag.delete()
    print("Tag deleted: old-release")
    
except Exception as e:
    print(f"Delete failed: {e}")
```

### Bulk Delete Tags

Delete multiple tags matching a pattern:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

# Delete all pre-release tags
for tag in repo.tags():
    if "rc" in tag.id or "alpha" in tag.id or "beta" in tag.id:
        try:
            tag.delete()
            print(f"Deleted: {tag.id}")
        except Exception as e:
            print(f"Failed to delete {tag.id}: {e}")
```

## Real-World Workflows

### Release Tagging Workflow

Create and manage versioned releases:

```python
import lakefs
from datetime import datetime

def create_release_tag(repo_name, version, release_notes=""):
    """
    Create a release tag with metadata
    """
    repo = lakefs.repository(repo_name)
    tag_name = f"v{version}"
    
    try:
        # Create tag pointing to main
        tag = repo.tag(tag_name).create(source_ref="main")
        
        # Get commit info
        commit = tag.get_commit()
        
        print(f"Release Created: {tag_name}")
        print(f"Commit: {commit.id[:8]}")
        print(f"Message: {commit.message}")
        print(f"Date: {datetime.fromtimestamp(commit.creation_date)}")
        
        return tag_name
        
    except Exception as e:
        print(f"Failed to create release: {e}")
        return None


# Usage:
release = create_release_tag("prod-repo", "2.1.0", "New features and bug fixes")
if release:
    print(f"Release {release} is ready")
```

### Data Version Snapshots

Create snapshots of data at important milestones:

```python
import lakefs
import time

def snapshot_data_version(repo_name, version_name, description):
    """
    Take a snapshot of the current state and tag it
    """
    repo = lakefs.repository(repo_name)
    main = repo.branch("main")
    
    try:
        # Get current state
        current_commit = main.get_commit()
        
        # Create tag with metadata
        timestamp = int(time.time())
        tag = repo.tag(version_name).create(source_ref="main")
        
        print(f"Snapshot created: {version_name}")
        print(f"  Description: {description}")
        print(f"  Timestamp: {timestamp}")
        print(f"  Commit: {current_commit.id[:8]}")
        
        # List what's in this snapshot
        obj_count = 0
        for _ in repo.ref(version_name).objects():
            obj_count += 1
        print(f"  Objects: {obj_count}")
        
        return tag
        
    except Exception as e:
        print(f"Snapshot failed: {e}")
        return None


# Usage:
snapshot = snapshot_data_version(
    "data-repo",
    "snapshot-2024-01-15",
    "Complete customer dataset with all validations"
)
```

### ML Model Release Workflow

Release trained models with versioning:

```python
import lakefs
import json

def release_ml_model(repo_name, model_version, model_metrics):
    """
    Create a versioned release of an ML model
    """
    repo = lakefs.repository(repo_name)
    
    try:
        # Create release tag
        tag_name = f"model-v{model_version}"
        tag = repo.tag(tag_name).create(source_ref="main")
        
        commit = tag.get_commit()
        
        print(f"ML Model Released: {tag_name}")
        print(f"  Commit: {commit.id[:8]}")
        
        # Read model metadata from tagged version
        tag_ref = repo.ref(tag_name)
        
        try:
            with tag_ref.object("models/metadata.json").reader() as f:
                metadata = json.load(f)
                print(f"  Model: {metadata.get('name')}")
                print(f"  Framework: {metadata.get('framework')}")
                print(f"  Version: {metadata.get('version')}")
        except:
            print("  (No metadata file)")
        
        # Store release info
        release_info = {
            "version": model_version,
            "commit": commit.id,
            "metrics": model_metrics,
            "tag": tag_name
        }
        
        return release_info
        
    except Exception as e:
        print(f"Model release failed: {e}")
        return None


# Usage:
metrics = {
    "accuracy": 0.945,
    "precision": 0.92,
    "recall": 0.96,
    "f1": 0.939
}

release_info = release_ml_model("ml-repo", "3.2.0", metrics)
if release_info:
    print(f"\nModel released and can be retrieved from tag: {release_info['tag']}")
```

### Production Deployment Workflow

Manage production data versions:

```python
import lakefs

def promote_to_production(repo_name, from_tag, environment):
    """
    Promote a tagged version to production by creating an environment tag
    """
    repo = lakefs.repository(repo_name)
    
    try:
        # Create environment-specific tag
        env_tag_name = f"prod-{environment}"
        
        # Delete old environment tag if it exists
        try:
            old_tag = repo.tag(env_tag_name)
            old_tag.delete()
            print(f"Removed old {env_tag_name} tag")
        except:
            pass  # Tag didn't exist
        
        # Create new environment tag pointing to the same commit as version tag
        source_tag = repo.tag(from_tag)
        env_tag = repo.tag(env_tag_name).create(source_ref=source_tag)
        
        print(f"Promoted to {environment}")
        print(f"  Source: {from_tag}")
        print(f"  Target: {env_tag_name}")
        print(f"  Commit: {env_tag.get_commit().id[:8]}")
        
        return env_tag_name
        
    except Exception as e:
        print(f"Promotion failed: {e}")
        return None


# Usage:
env_tag = promote_to_production("prod-repo", "v2.1.0", "us-west-1")
if env_tag:
    print(f"Production data updated to use {env_tag}")
```

### Track Data Lineage with Tags

Create tags at each transformation stage:

```python
import lakefs

def track_data_lineage(repo_name):
    """
    Create tags at each stage of data transformation pipeline
    """
    repo = lakefs.repository(repo_name)
    main = repo.branch("main")
    
    stages = [
        "lineage-raw-data",
        "lineage-cleaned",
        "lineage-validated",
        "lineage-transformed",
        "lineage-production-ready"
    ]
    
    lineage = {}
    
    for stage in stages:
        try:
            # Check if stage tag exists
            tag = repo.tag(stage)
            tag.delete()
        except:
            pass  # Tag doesn't exist
        
        # Create new tag at this stage
        try:
            tag = repo.tag(stage).create(source_ref="main")
            commit = tag.get_commit()
            lineage[stage] = commit.id
            
            print(f"Tagged {stage}: {commit.id[:8]}")
        except Exception as e:
            print(f"Failed to tag {stage}: {e}")
    
    return lineage


# Usage:
lineage = track_data_lineage("analytics-repo")
print("\nData transformation lineage:")
for stage, commit_id in lineage.items():
    print(f"  {stage}: {commit_id}")
```

## Querying Tags by Commit Reference

### Find All Tags for a Commit

Find which tags point to a specific commit:

```python
import lakefs

def find_tags_for_commit(repo_name, commit_id):
    """Find all tags pointing to a specific commit"""
    repo = lakefs.repository(repo_name)
    
    matching_tags = []
    
    for tag in repo.tags():
        try:
            tag_commit = tag.get_commit()
            if tag_commit.id == commit_id:
                matching_tags.append(tag.id)
        except:
            pass
    
    return matching_tags


# Usage:
tags = find_tags_for_commit("my-repo", "abc123def456")
print(f"Tags pointing to abc123def456: {tags}")
```

### List Tags in Reverse Chronological Order

Get the most recent tags first:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

# Get all tags and sort by commit creation date
tags_with_date = []

for tag in repo.tags():
    try:
        commit = tag.get_commit()
        tags_with_date.append({
            "name": tag.id,
            "commit": commit.id,
            "date": commit.creation_date,
            "message": commit.message
        })
    except:
        pass

# Sort by creation date (newest first)
tags_with_date.sort(key=lambda x: x["date"], reverse=True)

print("Recent tags:")
for tag_info in tags_with_date[:10]:
    print(f"  {tag_info['name']:20} {tag_info['date']} {tag_info['message'][:40]}")
```

## Production Release Workflow

### Tagging a Production Release

Create and manage production releases with tags:

```python
import lakefs

repo = lakefs.repository("prod-repo")
main_branch = repo.branch("main")

# Tag the current commit for production
tag = repo.tag("prod-2024-01-15").create(source_ref=main_branch)

# Retrieve data from that tag later
tagged_ref = repo.ref("prod-2024-01-15")
for obj in tagged_ref.objects(prefix="models/"):
    print(f"Model: {obj.path}")
```

This workflow ensures:

- Production data is immutably tagged for audit trails
- You can always access exactly which version was deployed
- Data lineage is preserved for compliance and troubleshooting

## Error Handling

### Handling Common Tag Errors

```
