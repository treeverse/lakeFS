---
title: Python - Branches & Merging
description: Work with branches in lakeFS using the Python SDK for feature branching, experimentation, and collaboration
---

# Working with Branches & Merging

Branches are the foundation of version control in lakeFS. They enable parallel development, safe experimentation, and collaborative data management. This guide covers all branch operations using the Python SDK.

### Understanding Repositories and Branches

<h4>Repositories</h4>

A repository is a versioned storage namespace that holds all your data and version history. Create a repository by specifying a storage location:

```python
import lakefs

repo = lakefs.repository("my-repo").create(
    storage_namespace="s3://my-bucket/data"
)
```

<h4>Branches</h4>

Branches enable parallel development and experimentation. Each branch maintains its own version of the data:

```python
branch = lakefs.repository("my-repo").branch("main")
experiment_branch = lakefs.repository("my-repo").branch("experiment1").create(
    source_reference="main"
)
```

### Creating and Listing Branches

<h4>Creating a Branch</h4>

Create a new branch from any reference (branch, tag, or commit):

```python
import lakefs

# Initialize repository
repo = lakefs.repository("my-data-repo")

# Create a branch from main
experiment_branch = repo.branch("experiment-1").create(source_reference="main")

print(f"Created branch: {experiment_branch.id}")

# Check what commit this branch points to
commit = experiment_branch.get_commit()
print(f"Branch head: {commit.id}")
```

**Using an explicit client:**

```python
import lakefs
from lakefs.client import Client

# Create client
client = Client(
    host="http://localhost:8000",
    username="your-access-key",
    password="your-secret-key"
)

# Create branch with explicit client
branch = lakefs.repository("my-repo", client=client).branch("dev").create(
    source_reference="main"
)
```

<h4>Listing Branches</h4>

List all branches in a repository:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

print("All branches:")
for branch in repo.branches():
    commit = branch.get_commit()
    print(f"  {branch.id:20} -> {commit.id[:8]}... ({commit.message})")
```

<h4>Checking Branch Head</h4>

Get the commit a branch is currently pointing to:

```python
import lakefs

branch = lakefs.repository("my-data-repo").branch("main")

# Get the head commit
head_commit = branch.get_commit()
print(f"Current head: {head_commit.id}")
print(f"Committed by: {head_commit.committer}")
print(f"Message: {head_commit.message}")

# Or use the head property
head_ref = branch.head
print(f"Head reference ID: {head_ref.id}")
```

### Working with Branch Content

<h4>Viewing Uncommitted Changes</h4>

See what's changed on a branch since the last commit:

```python
import lakefs

branch = lakefs.repository("my-data-repo").branch("feature-branch")

# List uncommitted changes
print("Uncommitted changes:")
for change in branch.uncommitted():
    print(f"  {change.type:10} {change.path} ({change.size_bytes} bytes)")

# Count changes
change_count = len(list(branch.uncommitted()))
print(f"Total changes: {change_count}")

# Filter changes by prefix
data_changes = [c for c in branch.uncommitted() if c.path.startswith("data/")]
print(f"Changes in data/ folder: {len(data_changes)}")
```

<h4>Committing Changes</h4>

Create a commit with your changes:

```python
import lakefs

branch = lakefs.repository("my-data-repo").branch("feature-branch")

# Upload some data first
branch.object("data/dataset.csv").upload(data=b"id,value\n1,100\n2,200")

# Commit with message and metadata
ref = branch.commit(
    message="Add customer dataset",
    metadata={
        "author": "data-team",
        "version": "1.0",
        "dataset-type": "raw"
    }
)

print(f"Committed: {ref.id}")
print(f"Message: {ref.get_commit().message}")
print(f"Metadata: {ref.get_commit().metadata}")
```

### Comparing Branches

<h4>Diff Between References</h4>

See what changed between two branches or commits:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
main_branch = repo.branch("main")
feature_branch = repo.branch("feature-add-models")

# Compare branches
print("Changes in feature-add-models vs main:")
for change in main_branch.diff(other_ref=feature_branch):
    print(f"  {change.type:10} {change.path}")

# Count different types of changes
changes = list(main_branch.diff(other_ref=feature_branch))
added = len([c for c in changes if c.type == "added"])
removed = len([c for c in changes if c.type == "removed"])
modified = len([c for c in changes if c.type == "modified"])

print(f"Added: {added}, Removed: {removed}, Modified: {modified}")
```

### Merging Branches

<h4>Merging Into Another Branch</h4>

Merge changes from one branch into another:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
feature_branch = repo.branch("feature-branch")
main_branch = repo.branch("main")

try:
    # Merge feature branch into main
    merge_result = feature_branch.merge_into(main_branch)
    print(f"Merge successful: {merge_result}")

    # Verify merge by checking that differences are gone
    remaining_diffs = list(main_branch.diff(other_ref=feature_branch))
    print(f"Remaining differences: {len(remaining_diffs)}")

except Exception as e:
    print(f"Merge failed: {e}")
    # Resolve conflicts or adjust data and try again
```

<h4>Merge with Conflict Detection</h4>

```python
import lakefs

repo = lakefs.repository("my-data-repo")

try:
    branch1 = repo.branch("feature-1")
    branch2 = repo.branch("feature-2")
    main = repo.branch("main")

    # Check for conflicts before merging
    conflicts = list(main.diff(other_ref=branch1))

    if conflicts:
        print(f"Potential conflicts: {len(conflicts)} changes")
        for change in conflicts[:5]:  # Show first 5
            print(f"  {change.type}: {change.path}")

    # Proceed with merge if acceptable
    merge_commit = branch1.merge_into(main)
    print(f"Merged into main: {merge_commit}")

except Exception as e:
    print(f"Merge error: {e}")
```

### Cherry-Picking Commits

Apply a specific commit from one branch to another:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
main_branch = repo.branch("main")
release_branch = repo.branch("release-v1.0")

try:
    # Cherry-pick a specific commit onto release branch
    # First, get a commit ID from main
    main_commits = list(main_branch.log(max_amount=5))
    if main_commits:
        commit_to_cherry_pick = main_commits[0]

        # Cherry-pick it onto release branch
        new_commit = release_branch.cherry_pick(commit_to_cherry_pick.id)
        print(f"Cherry-picked commit: {new_commit.id}")
        print(f"Message: {new_commit.message}")

except Exception as e:
    print(f"Cherry-pick failed: {e}")
```

### Reverting Changes

<h4>Revert a Commit</h4>

Undo the changes from a specific commit:

```python
import lakefs

repo = lakefs.repository("my-data-repo")
branch = repo.branch("develop")

try:
    # Get recent commits
    recent_commits = list(branch.log(max_amount=10))

    if len(recent_commits) > 1:
        # Revert the most recent commit
        commit_to_revert = recent_commits[0]

        revert_result = branch.revert(reference=commit_to_revert.id)
        print(f"Reverted commit: {commit_to_revert.id}")
        print(f"New commit created: {revert_result.id}")

except Exception as e:
    print(f"Revert failed: {e}")
```

<h4>Reverting Merge Commits</h4>

When reverting a merge commit, you can specify which parent to revert against:

```python
import lakefs

branch = lakefs.repository("my-data-repo").branch("main")

try:
    # Get commit history
    commits = list(branch.log(max_amount=5))

    for commit in commits:
        # If this is a merge commit (has multiple parents)
        if len(commit.parents) > 1:
            # Revert against parent 1 (the original main)
            result = branch.revert(reference=commit.id, parent_number=1)
            print(f"Reverted merge commit: {commit.id}")
            break

except Exception as e:
    print(f"Error: {e}")
```

### Resetting Branches

<h4>Reset Uncommitted Changes</h4>

Discard uncommitted changes on a branch:

```python
import lakefs

branch = lakefs.repository("my-data-repo").branch("feature-branch")

# Show uncommitted changes before reset
changes_before = list(branch.uncommitted())
print(f"Uncommitted changes before reset: {len(changes_before)}")

try:
    # Reset all changes
    branch.reset_changes(path_type="reset")

    # Verify changes are gone
    changes_after = list(branch.uncommitted())
    print(f"Uncommitted changes after reset: {len(changes_after)}")

except Exception as e:
    print(f"Reset failed: {e}")
```

<h4>Reset Changes for Specific Paths</h4>

Reset changes only for certain paths or prefixes:

```python
import lakefs

branch = lakefs.repository("my-data-repo").branch("develop")

try:
    # Reset changes for a specific object
    branch.reset_changes(path_type="object", path="data/temp.csv")
    print("Reset: data/temp.csv")

    # Reset all changes in a folder (common prefix)
    branch.reset_changes(path_type="common_prefix", path="logs/")
    print("Reset: logs/*")

    # Verify remaining changes
    remaining = list(branch.uncommitted())
    print(f"Remaining changes: {len(remaining)}")

except Exception as e:
    print(f"Reset error: {e}")
```

### Deleting Branches

<h4>Delete a Branch</h4>

Remove a branch that's no longer needed:

```python
import lakefs

repo = lakefs.repository("my-data-repo")

try:
    # Delete a branch
    branch = repo.branch("old-experiment")
    branch.delete()
    print("Branch deleted: old-experiment")

except Exception as e:
    print(f"Delete failed: {e}")

# Verify it's gone
remaining_branches = [b.id for b in repo.branches()]
print(f"Remaining branches: {remaining_branches}")
```

## Real-World Workflows

### Isolated Dev/Test Environment

<h4>Feature Branch Workflow</h4>

Implement a typical feature development workflow:

```python
import lakefs

def feature_workflow(repo_name, feature_name, data_updates):
    """
    Complete feature branch workflow:
    1. Create feature branch
    2. Make changes
    3. Test/commit
    4. Merge to main
    """
    repo = lakefs.repository(repo_name)
    main = repo.branch("main")
    feature = repo.branch(feature_name)

    try:
        # 1. Create feature branch from main
        feature.create(source_reference="main")
        print(f"Created feature branch: {feature_name}")

        # 2. Make changes
        for path, data in data_updates.items():
            feature.object(path).upload(data=data)
        print(f"Uploaded {len(data_updates)} objects")

        # 3. Review changes
        changes = list(feature.uncommitted())
        print(f"Changes to commit: {len(changes)}")

        # 4. Commit
        commit_ref = feature.commit(
            message=f"Implement {feature_name}",
            metadata={"feature": feature_name}
        )
        print(f"Committed: {commit_ref.id}")

        # 5. Verify diff before merge
        diff = list(main.diff(other_ref=feature))
        print(f"Ready to merge {len(diff)} changes")

        # 6. Merge to main
        merge_result = feature.merge_into(main)
        print(f"Merged to main: {merge_result}")

        # 7. Cleanup
        feature.delete()
        print(f"Deleted feature branch: {feature_name}")

        return True

    except Exception as e:
        print(f"Feature workflow failed: {e}")
        # Cleanup on error
        try:
            feature.delete()
        except:
            pass
        return False


# Usage:
feature_workflow(
    repo_name="analytics-repo",
    feature_name="add-customer-metrics",
    data_updates={
        "data/metrics/customer_v2.csv": b"id,value\n1,100\n2,200",
        "data/metrics/metadata.json": b'{"version": "2", "date": "2024-01-15"}'
    }
)
```

<h4>Experimentation Branch Workflow</h4>

Create isolated branches for experiments:

```python
import lakefs
import time

def create_experiment(repo_name, experiment_name, experiment_logic):
    """Run an experiment in isolation, keep results if successful"""
    repo = lakefs.repository(repo_name)
    exp_branch = repo.branch(experiment_name)

    try:
        # Create isolated experiment branch
        exp_branch.create(source_reference="main")
        print(f"Started experiment: {experiment_name}")

        # Run experiment logic
        experiment_logic(exp_branch)

        # Commit results
        changes = list(exp_branch.uncommitted())
        if changes:
            exp_branch.commit(
                message=f"Results from {experiment_name}",
                metadata={"experiment": experiment_name, "timestamp": str(time.time())}
            )
            print(f"Experiment successful, committed {len(changes)} changes")

        return exp_branch.get_commit().id

    except Exception as e:
        print(f"Experiment failed: {e}")
        # Clean up failed experiment
        try:
            exp_branch.delete()
        except:
            pass
        return None


# Example experiment
def model_training_experiment(branch):
    # Simulate model training
    branch.object("models/trained_v2.pkl").upload(data=b"model_binary_data")
    branch.object("logs/training.log").upload(data=b"Training complete")


# Usage:
result_commit = create_experiment(
    repo_name="ml-repo",
    experiment_name="neural-net-v3",
    experiment_logic=model_training_experiment
)

if result_commit:
    print(f"Keep results from commit: {result_commit}")
```

<h4>Release Branch Management</h4>

Manage release versions with branches and tags:

```python
import lakefs

def create_release(repo_name, version_tag):
    """Create a release: branch from main, prepare, merge, tag"""
    repo = lakefs.repository(repo_name)
    main = repo.branch("main")
    release_branch = repo.branch(f"release-{version_tag}")

    try:
        # 1. Create release branch
        release_branch.create(source_reference="main")
        print(f"Created release branch: release-{version_tag}")

        # 2. Any release-specific changes (version bumps, etc)
        release_branch.object("VERSION").upload(data=version_tag.encode())
        release_branch.object("RELEASE_NOTES.md").upload(
            data=f"# Release {version_tag}\nDate: 2024-01-15".encode()
        )

        # 3. Commit release changes
        release_ref = release_branch.commit(
            message=f"Release {version_tag}",
            metadata={"version": version_tag, "type": "release"}
        )

        # 4. Merge back to main
        release_branch.merge_into(main)
        print(f"Merged release to main")

        # 5. Create tag for this release
        tag = repo.tag(f"v{version_tag}").create(source_ref=release_ref)
        print(f"Tagged release: v{version_tag}")

        # 6. Clean up release branch
        release_branch.delete()
        print(f"Cleaned up release branch")

        return f"v{version_tag}"

    except Exception as e:
        print(f"Release creation failed: {e}")
        return None


# Usage:
release_tag = create_release("prod-repo", "1.5.0")
if release_tag:
    print(f"Release ready: {release_tag}")
```

## Error Handling

<h4>Handling Common Branch Errors</h4>

```python
import lakefs
from lakefs.exceptions import NotFoundException, ConflictException, ForbiddenException

repo = lakefs.repository("my-data-repo")

# Branch already exists
try:
    branch = repo.branch("main").create(source_reference="main", exist_ok=False)
except ConflictException:
    print("Branch already exists")
    branch = repo.branch("main")

# Branch doesn't exist
try:
    branch = repo.branch("non-existent")
    commit = branch.get_commit()
except NotFoundException:
    print("Branch not found")

# Protected branch (cannot delete)
try:
    repo.branch("main").delete()
except ForbiddenException:
    print("Cannot delete protected branch")

# Source reference not found
try:
    branch = repo.branch("new-branch").create(
        source_reference="non-existent-ref"
    )
except NotFoundException:
    print("Source reference does not exist")
```
