---
title: Python - References, Commits & Tags
description: Navigate commit history, understand references, manage tags, and track data lineage in lakeFS with Python
---

# Working with References, Commits & Tags

References, commits, and tags are fundamental to understanding and managing versions in lakeFS. This guide covers navigating commit history, working with references, creating immutable snapshots with tags, and using metadata for tracking and lineage.

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
changed = [c for c in all_changes if c.type == "changed"]

print(f"Added: {len(added)}")
print(f"Removed: {len(removed)}")
print(f"Changed: {len(changed)}")

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

## Working with Tags

Tags are immutable pointers to specific commits in lakeFS, making them perfect for marking releases, data versions, and important snapshots.

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

## Listing Tags

List all tags in a repository:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

print("All tags in repository:")
for tag in repo.tags():
    commit = tag.get_commit()
    print(f"  {tag.id:20} -> {commit.id[:8]}... ({commit.message})")
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

### Compare Data Across Tagged Versions

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
changed = len([c for c in changes if c.type == "changed"])

print(f"\nSummary: +{added} -{removed} ~{changed}")
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

Follow a commit back through its parents. This is for better understanding of commit, we will prefer to use `log` operation to trace back changes:

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

### Handling Tag Errors

```python
import lakefs
from lakefs.exceptions import ConflictException, NotFoundException

repo = lakefs.repository("my-data-repo")

# Tag already exists
try:
    tag = repo.tag("v1.0.0").create(source_ref="main", exist_ok=False)
except ConflictException:
    print("Tag already exists")
    tag = repo.tag("v1.0.0")

# Tag doesn't exist
try:
    tag = repo.tag("non-existent-tag")
    commit = tag.get_commit()
except NotFoundException:
    print("Tag not found")
```
