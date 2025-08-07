---
title: Branches and Commits
description: Version control operations with branches and commits
sdk_types: ["high-level"]
difficulty: "intermediate"
use_cases: ["version-control", "branching", "merging", "commits"]
topics: ["branches", "commits", "merging", "version-control"]
audience: ["data-engineers", "developers", "python-developers"]
last_updated: "2024-01-15"
---

# Branches and Commits

Master version control operations using branches and commits in the High-Level Python SDK. Branches provide isolated development environments while commits create immutable snapshots of your data.

## Branch Concepts

### Branch vs Reference
- **Branch**: A mutable pointer to a commit that can be updated with new commits
- **Reference**: A read-only pointer to any commit, branch, or tag
- **Head**: The latest commit on a branch

### Branch Lifecycle
1. **Create** - Branch from existing reference
2. **Modify** - Add, update, or delete objects
3. **Commit** - Create immutable snapshot
4. **Merge** - Integrate changes into target branch
5. **Delete** - Remove branch when no longer needed

## Creating Branches

### Basic Branch Creation

```python
import lakefs

repo = lakefs.repository("my-repo")

# Create branch from main
feature_branch = repo.branch("feature-branch").create(source_reference="main")

print(f"Created branch: {feature_branch.id}")
print(f"Source commit: {feature_branch.get_commit().id}")
```

**Expected Output:**
```
Created branch: feature-branch
Source commit: c7a632d74f46c...
```

### Branch from Specific References

```python
# Create branch from specific commit
hotfix_branch = repo.branch("hotfix-v1.2").create(
    source_reference="a1b2c3d4e5f6..."
)

# Create branch from tag
release_branch = repo.branch("release-prep").create(
    source_reference="v1.1.0"
)

# Create branch from ref expression
debug_branch = repo.branch("debug-issue").create(
    source_reference="main~5"  # 5 commits before main
)
```

### Safe Branch Creation

```python
from lakefs.exceptions import ConflictException

def create_branch_safely(repo, branch_name, source_ref):
    try:
        # Try to create new branch
        branch = repo.branch(branch_name).create(
            source_reference=source_ref,
            exist_ok=False
        )
        print(f"Created new branch: {branch_name}")
        return branch
        
    except ConflictException:
        print(f"Branch {branch_name} already exists, using existing branch")
        return repo.branch(branch_name)

# Usage
branch = create_branch_safely(repo, "feature-auth", "main")
```

### Branch Creation with exist_ok

```python
# Create branch or use existing one
branch = repo.branch("development").create(
    source_reference="main",
    exist_ok=True  # Don't fail if branch exists
)

print(f"Branch ready: {branch.id}")
```

## Branch Information and Navigation

### Accessing Branch Properties

```python
branch = repo.branch("main")

# Get current commit
commit = branch.get_commit()
print(f"Branch: {branch.id}")
print(f"Repository: {branch.repo_id}")
print(f"Current commit: {commit.id}")
print(f"Commit message: {commit.message}")
print(f"Committer: {commit.committer}")
print(f"Commit date: {commit.creation_date}")

# Access commit metadata
if commit.metadata:
    print("Commit metadata:")
    for key, value in commit.metadata.items():
        print(f"  {key}: {value}")
```

**Expected Output:**
```
Branch: main
Repository: my-repo
Current commit: c7a632d74f46c...
Commit message: Initial commit
Committer: admin
Commit date: 1640995200
Commit metadata:
  version: 1.0.0
  environment: production
```

### Branch Head Reference

```python
# Get head reference (always latest commit)
head_ref = branch.head
print(f"Head commit: {head_ref.id}")

# Head is automatically updated after commits
branch.object("new-file.txt").upload(data="content")
branch.commit("Add new file")

new_head = branch.head
print(f"New head commit: {new_head.id}")
print(f"Head changed: {head_ref.id != new_head.id}")
```

### Listing Branches

```python
# List all branches in repository
print("All branches:")
for branch in repo.branches():
    commit = branch.get_commit()
    print(f"  {branch.id} -> {commit.id[:8]} ({commit.message})")

# List branches with filtering
print("\nFeature branches:")
for branch in repo.branches(prefix="feature-"):
    print(f"  {branch.id}")

# List recent branches with pagination
print("\nRecent branches:")
for branch in repo.branches(max_amount=5):
    print(f"  {branch.id}")
```

**Expected Output:**
```
All branches:
  main -> c7a632d7 (Initial commit)
  feature-auth -> a1b2c3d4 (Add authentication)
  hotfix-v1.2 -> f6e5d4c3 (Fix critical bug)

Feature branches:
  feature-auth
  feature-dashboard

Recent branches:
  main
  feature-auth
  hotfix-v1.2
  development
  staging
```

## Commit Operations

### Creating Commits

```python
branch = repo.branch("feature-branch")

# Upload some data first
branch.object("data/users.csv").upload(
    data="name,email\nAlice,alice@example.com\nBob,bob@example.com"
)
branch.object("config/settings.json").upload(
    data='{"version": "2.0", "debug": false}'
)

# Simple commit
commit = branch.commit(message="Add user data and configuration")
print(f"Commit ID: {commit.id}")
print(f"Message: {commit.message}")
```

### Commits with Metadata

```python
# Commit with rich metadata
commit = branch.commit(
    message="Update data pipeline for Q4 processing",
    metadata={
        "version": "1.2.0",
        "author": "data-team",
        "ticket": "PROJ-123",
        "environment": "staging",
        "reviewed_by": "senior-engineer"
    }
)

print(f"Commit: {commit.id}")
print("Metadata:")
for key, value in commit.metadata.items():
    print(f"  {key}: {value}")
```

**Expected Output:**
```
Commit: a1b2c3d4e5f6...
Metadata:
  version: 1.2.0
  author: data-team
  ticket: PROJ-123
  environment: staging
  reviewed_by: senior-engineer
```

### Viewing Uncommitted Changes

```python
# Make some changes
branch.object("data/new-file.txt").upload(data="New content")
branch.object("data/existing-file.txt").upload(data="Updated content")

# Check uncommitted changes
changes = list(branch.uncommitted())
print(f"Uncommitted changes: {len(changes)}")

for change in changes:
    print(f"  {change.type}: {change.path}")
    if change.size_bytes:
        print(f"    Size: {change.size_bytes} bytes")
    print(f"    Type: {change.path_type}")
```

**Expected Output:**
```
Uncommitted changes: 2
  added: data/new-file.txt
    Size: 11 bytes
    Type: object
  changed: data/existing-file.txt
    Size: 15 bytes
    Type: object
```

### Commit History and Logs

```python
# Get commit history
print("Recent commits:")
for commit in branch.log(max_amount=5):
    print(f"  {commit.id[:8]} - {commit.message}")
    print(f"    By: {commit.committer}")
    print(f"    Date: {commit.creation_date}")
    if commit.parents:
        print(f"    Parents: {[p[:8] for p in commit.parents]}")
    print()
```

**Expected Output:**
```
Recent commits:
  a1b2c3d4 - Update data pipeline for Q4 processing
    By: data-team
    Date: 1640995200
    Parents: ['c7a632d7']

  c7a632d7 - Add user data and configuration
    By: admin
    Date: 1640908800
    Parents: ['f6e5d4c3']
```

## Branch Comparison and Diffing

### Comparing Branches

```python
main_branch = repo.branch("main")
feature_branch = repo.branch("feature-branch")

# Compare branches
print("Changes in feature branch vs main:")
for change in main_branch.diff(other_ref=feature_branch):
    print(f"  {change.type}: {change.path}")
    if change.size_bytes:
        print(f"    Size: {change.size_bytes} bytes")

# Compare with specific commit
print("\nChanges since last release:")
for change in main_branch.diff(other_ref="v1.0.0"):
    print(f"  {change.type}: {change.path}")
```

**Expected Output:**
```
Changes in feature branch vs main:
  added: data/users.csv
    Size: 58 bytes
  added: config/settings.json
    Size: 35 bytes
  changed: README.md
    Size: 1024 bytes

Changes since last release:
  added: features/auth.py
    Size: 2048 bytes
  changed: config/app.yaml
    Size: 512 bytes
```

### Advanced Diff Operations

```python
# Diff with filtering
print("Changes to data files:")
for change in main_branch.diff(other_ref=feature_branch, prefix="data/"):
    print(f"  {change.type}: {change.path}")

# Diff with common prefixes
print("\nChanges by directory:")
for change in main_branch.diff(other_ref=feature_branch, delimiter="/"):
    if hasattr(change, 'path'):
        print(f"  {change.type}: {change.path}")
    else:
        print(f"  directory: {change}")
```

## Merging Operations

### Basic Merging

```python
feature_branch = repo.branch("feature-auth")
main_branch = repo.branch("main")

# Merge feature branch into main
merge_commit_id = feature_branch.merge_into(main_branch)
print(f"Merge commit: {merge_commit_id}")

# Get the merge commit details
merge_commit = main_branch.get_commit()
print(f"Merge message: {merge_commit.message}")
print(f"Parents: {merge_commit.parents}")
```

**Expected Output:**
```
Merge commit: b2c3d4e5f6a7...
Merge message: Merge 'feature-auth' into 'main'
Parents: ['a1b2c3d4e5f6', 'c7a632d74f46']
```

### Merge with Custom Message

```python
# Merge with custom commit message and metadata
merge_commit_id = feature_branch.merge_into(
    main_branch,
    message="Integrate authentication feature",
    metadata={
        "feature": "authentication",
        "reviewer": "senior-dev",
        "tests_passed": "true"
    }
)

print(f"Custom merge commit: {merge_commit_id}")
```

### Handling Merge Conflicts

```python
from lakefs.exceptions import ConflictException

def safe_merge(source_branch, target_branch, message=None):
    try:
        merge_commit = source_branch.merge_into(
            target_branch,
            message=message or f"Merge {source_branch.id} into {target_branch.id}"
        )
        print(f"Merge successful: {merge_commit}")
        return merge_commit
        
    except ConflictException as e:
        print(f"Merge conflict detected: {e}")
        print("Manual conflict resolution required")
        return None

# Usage
result = safe_merge(feature_branch, main_branch)
```

## Advanced Branch Operations

### Cherry-picking Commits

```python
# Cherry-pick a specific commit to current branch
source_commit = "a1b2c3d4e5f6..."
cherry_picked_commit = branch.cherry_pick(reference=source_commit)

print(f"Cherry-picked commit: {cherry_picked_commit.id}")
print(f"Original message: {cherry_picked_commit.message}")

# Cherry-pick from another branch
feature_branch = repo.branch("feature-experimental")
latest_commit = feature_branch.get_commit()
cherry_picked = branch.cherry_pick(reference=latest_commit)
```

### Reverting Changes

```python
# Revert a specific commit
commit_to_revert = "a1b2c3d4e5f6..."
revert_commit = branch.revert(reference=commit_to_revert)

print(f"Revert commit: {revert_commit.id}")
print(f"Revert message: {revert_commit.message}")

# Revert merge commit (specify parent)
merge_commit = "b2c3d4e5f6a7..."
revert_commit = branch.revert(
    reference=merge_commit,
    parent_number=1  # Revert to first parent
)
```

### Resetting Changes

```python
# Reset all uncommitted changes
branch.reset_changes(path_type="reset")
print("All changes reset")

# Reset specific object
branch.reset_changes(
    path_type="object",
    path="data/file-to-reset.txt"
)
print("Specific file reset")

# Reset common prefix (directory)
branch.reset_changes(
    path_type="common_prefix", 
    path="data/temp/"
)
print("Directory reset")
```

### Deleting Branches

```python
from lakefs.exceptions import ForbiddenException

def delete_branch_safely(repo, branch_name):
    try:
        branch = repo.branch(branch_name)
        branch.delete()
        print(f"Branch {branch_name} deleted successfully")
        return True
        
    except ForbiddenException:
        print(f"Branch {branch_name} is protected and cannot be deleted")
        return False
    except Exception as e:
        print(f"Error deleting branch {branch_name}: {e}")
        return False

# Usage
delete_branch_safely(repo, "feature-completed")
```

## Branch Protection and Versioning

### Understanding Branch Protection

```python
# Attempt operations on protected branches
def check_branch_protection(branch):
    try:
        # Try to commit to potentially protected branch
        branch.object("test-file.txt").upload(data="test")
        commit = branch.commit("Test commit")
        print(f"Commit successful: {commit.id}")
        
        # Clean up test
        branch.reset_changes(path_type="reset")
        
    except ForbiddenException:
        print(f"Branch {branch.id} is protected")
    except Exception as e:
        print(f"Error: {e}")

# Check main branch protection
main_branch = repo.branch("main")
check_branch_protection(main_branch)
```

### Versioning Concepts

```python
# Demonstrate versioning with ref expressions
branch = repo.branch("main")

# Current head
current = branch.get_commit()
print(f"Current: {current.id} - {current.message}")

# Previous commits using ref expressions
previous_commits = [
    repo.ref("main~1").get_commit(),  # 1 commit back
    repo.ref("main~2").get_commit(),  # 2 commits back
    repo.ref("main~3").get_commit(),  # 3 commits back
]

print("\nCommit history:")
for i, commit in enumerate(previous_commits):
    print(f"  main~{i+1}: {commit.id[:8]} - {commit.message}")
```

## Batch Operations and Performance

### Efficient Branch Operations

```python
def create_multiple_branches(repo, branch_configs):
    """Create multiple branches efficiently"""
    branches = []
    
    for config in branch_configs:
        try:
            branch = repo.branch(config['name']).create(
                source_reference=config['source'],
                exist_ok=True
            )
            branches.append(branch)
            print(f"Created/accessed branch: {config['name']}")
            
        except Exception as e:
            print(f"Failed to create branch {config['name']}: {e}")
    
    return branches

# Usage
branch_configs = [
    {'name': 'feature-api', 'source': 'main'},
    {'name': 'feature-ui', 'source': 'main'},
    {'name': 'hotfix-critical', 'source': 'v1.0.0'},
]

branches = create_multiple_branches(repo, branch_configs)
```

### Bulk Commit Operations

```python
def bulk_commit_changes(branch, files_data, commit_message):
    """Upload multiple files and commit in one operation"""
    
    # Upload all files
    for file_path, content in files_data.items():
        branch.object(file_path).upload(data=content)
    
    # Single commit for all changes
    commit = branch.commit(
        message=commit_message,
        metadata={"files_count": str(len(files_data))}
    )
    
    print(f"Committed {len(files_data)} files: {commit.id}")
    return commit

# Usage
files = {
    "data/users.csv": "name,email\nAlice,alice@example.com",
    "data/products.csv": "id,name,price\n1,Widget,10.99",
    "config/settings.json": '{"version": "1.0"}'
}

commit = bulk_commit_changes(
    branch, 
    files, 
    "Add initial data files and configuration"
)
```

## Error Handling and Best Practices

### Comprehensive Error Handling

```python
from lakefs.exceptions import (
    NotFoundException, 
    ConflictException, 
    ForbiddenException,
    NotAuthorizedException
)

def robust_branch_operations(repo, branch_name, source_ref):
    try:
        # Create branch
        branch = repo.branch(branch_name).create(
            source_reference=source_ref,
            exist_ok=False
        )
        print(f"Created branch: {branch_name}")
        
        # Make changes
        branch.object("data/test.txt").upload(data="test content")
        
        # Commit changes
        commit = branch.commit("Add test data")
        print(f"Committed: {commit.id}")
        
        return branch
        
    except ConflictException:
        print(f"Branch {branch_name} already exists")
        return repo.branch(branch_name)
        
    except NotFoundException:
        print(f"Source reference {source_ref} not found")
        return None
        
    except ForbiddenException:
        print(f"Operation forbidden - check branch protection rules")
        return None
        
    except NotAuthorizedException:
        print("Not authorized to perform this operation")
        return None
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Usage
branch = robust_branch_operations(repo, "feature-test", "main")
```

### Best Practices

```python
def branch_workflow_best_practices():
    """Demonstrate best practices for branch workflows"""
    
    repo = lakefs.repository("my-repo")
    
    # 1. Use descriptive branch names
    branch_name = "feature/user-authentication-v2"
    
    # 2. Always specify source reference explicitly
    branch = repo.branch(branch_name).create(
        source_reference="main",
        exist_ok=True
    )
    
    # 3. Check for uncommitted changes before major operations
    uncommitted = list(branch.uncommitted())
    if uncommitted:
        print(f"Warning: {len(uncommitted)} uncommitted changes")
    
    # 4. Use meaningful commit messages and metadata
    if uncommitted:
        commit = branch.commit(
            message="Implement user authentication with JWT tokens",
            metadata={
                "feature": "authentication",
                "version": "2.0",
                "tests": "passed",
                "reviewer": "security-team"
            }
        )
        print(f"Committed with metadata: {commit.id}")
    
    # 5. Clean up feature branches after merging
    main_branch = repo.branch("main")
    try:
        merge_commit = branch.merge_into(main_branch)
        print(f"Merged successfully: {merge_commit}")
        
        # Delete feature branch after successful merge
        branch.delete()
        print(f"Cleaned up branch: {branch_name}")
        
    except Exception as e:
        print(f"Merge failed: {e}")

# Run best practices example
branch_workflow_best_practices()
```

## Key Points

- **Lazy evaluation**: Branch objects don't connect to server until you access properties or call methods
- **Immutable commits**: Once created, commits cannot be modified
- **Branch protection**: Some branches may be protected from direct commits or deletion
- **Ref expressions**: Use Git-style expressions like `main~1` for relative references
- **Atomic operations**: Commits are atomic - either all changes are committed or none
- **Metadata support**: Both commits and merges support custom metadata

## See Also

- **[Repository Management](repositories.md)** - Creating and managing repositories
- **[Object Operations](objects-and-io.md)** - Working with files and data
- **[Transactions](transactions.md)** - Atomic multi-operation workflows
- **[Import Operations](imports-and-exports.md)** - Bulk data operations
- **[Best Practices](../reference/best-practices.md)** - Production deployment guidance