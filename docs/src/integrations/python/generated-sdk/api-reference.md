---
title: Generated SDK API Reference
description: Complete API reference for the Generated Python SDK
sdk_types: ["generated"]
difficulty: "intermediate"
use_cases: ["api-reference", "direct-api", "custom-operations"]
topics: ["api", "reference", "methods", "classes"]
audience: ["developers", "advanced-users", "integrators"]
last_updated: "2024-01-15"
---

# Generated SDK API Reference

Complete reference for the Generated Python SDK classes and methods.

## Client Initialization

### ApiClient Context Manager

The recommended way to use the Generated SDK is with the `ApiClient` context manager:

```python
import lakefs_sdk
from lakefs_sdk.rest import ApiException

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

with lakefs_sdk.ApiClient(configuration) as api_client:
    # Create API instances
    repositories_api = lakefs_sdk.RepositoriesApi(api_client)
    # Use the API...
```

### Configuration Class

The `Configuration` class manages all client settings:

```python
import lakefs_sdk

config = lakefs_sdk.Configuration(
    # Required settings
    host="http://localhost:8000",
    username="access_key_id",
    password="secret_access_key",
    
    # Optional authentication
    access_token="jwt_token",  # Alternative to username/password
    
    # SSL settings
    verify_ssl=True,
    ssl_ca_cert="/path/to/ca.pem",
    cert_file="/path/to/client.pem",
    key_file="/path/to/client.key",
    
    # Proxy settings
    proxy="http://proxy:8080",
    proxy_headers={"Proxy-Authorization": "Basic ..."},
    
    # Connection settings
    connection_pool_maxsize=10,
    user_agent="MyApp/1.0"
)
```

## API Classes

### RepositoriesApi

Manage lakeFS repositories and their settings.

#### `list_repositories(prefix=None, after=None, amount=None)`

List repositories with optional filtering and pagination.

**Parameters:**
- `prefix` (Optional[str]): Filter repositories by name prefix
- `after` (Optional[str]): Return results after this repository name
- `amount` (Optional[int]): Maximum number of results to return (default: 100)

**Returns:**
- `RepositoryList`: List of repositories with pagination info

**Example:**
```python
repositories_api = lakefs_sdk.RepositoriesApi(api_client)

# List all repositories
repos = repositories_api.list_repositories()
for repo in repos.results:
    print(f"Repository: {repo.id}")

# List with filtering
filtered_repos = repositories_api.list_repositories(
    prefix="data-",
    amount=50
)
```

#### `create_repository(repository_creation)`

Create a new repository.

**Parameters:**
- `repository_creation` (RepositoryCreation): Repository configuration

**Returns:**
- `Repository`: Created repository object

**Raises:**
- `ConflictException`: Repository already exists
- `ValidationException`: Invalid repository configuration

**Example:**
```python
from lakefs_sdk.models import RepositoryCreation

repo_creation = RepositoryCreation(
    name="my-data-lake",
    storage_namespace="s3://my-bucket/repos/my-data-lake",
    default_branch="main",
    sample_data=False
)

repo = repositories_api.create_repository(repository_creation=repo_creation)
print(f"Created repository: {repo.id}")
```

#### `get_repository(repository)`

Get repository details.

**Parameters:**
- `repository` (str): Repository name

**Returns:**
- `Repository`: Repository object

**Raises:**
- `NotFoundException`: Repository not found

**Example:**
```python
repo = repositories_api.get_repository(repository="my-repo")
print(f"Default branch: {repo.default_branch}")
print(f"Storage namespace: {repo.storage_namespace}")
```

#### `delete_repository(repository)`

Delete a repository permanently.

**Parameters:**
- `repository` (str): Repository name

**Raises:**
- `NotFoundException`: Repository not found
- `ValidationException`: Repository cannot be deleted

**Example:**
```python
repositories_api.delete_repository(repository="old-repo")
print("Repository deleted successfully")
```

### BranchesApi

Manage branches within repositories.

#### `list_branches(repository, prefix=None, after=None, amount=None)`

List branches in a repository.

**Parameters:**
- `repository` (str): Repository name
- `prefix` (Optional[str]): Filter branches by name prefix
- `after` (Optional[str]): Return results after this branch name
- `amount` (Optional[int]): Maximum number of results to return

**Returns:**
- `RefList`: List of branches with pagination info

**Example:**
```python
branches_api = lakefs_sdk.BranchesApi(api_client)

# List all branches
branches = branches_api.list_branches(repository="my-repo")
for branch in branches.results:
    print(f"Branch: {branch.id} (commit: {branch.commit_id})")

# List with filtering
feature_branches = branches_api.list_branches(
    repository="my-repo",
    prefix="feature-"
)
```

#### `create_branch(repository, branch_creation)`

Create a new branch.

**Parameters:**
- `repository` (str): Repository name
- `branch_creation` (BranchCreation): Branch configuration

**Returns:**
- `Ref`: Created branch object

**Raises:**
- `ConflictException`: Branch already exists
- `NotFoundException`: Source reference not found

**Example:**
```python
from lakefs_sdk.models import BranchCreation

branch_creation = BranchCreation(
    name="feature-new-analytics",
    source="main"  # Source branch or commit ID
)

branch = branches_api.create_branch(
    repository="my-repo",
    branch_creation=branch_creation
)
print(f"Created branch: {branch.id}")
```

#### `get_branch(repository, branch)`

Get branch details.

**Parameters:**
- `repository` (str): Repository name
- `branch` (str): Branch name

**Returns:**
- `Ref`: Branch object

**Raises:**
- `NotFoundException`: Branch not found

**Example:**
```python
branch = branches_api.get_branch(
    repository="my-repo",
    branch="feature-branch"
)
print(f"Branch commit: {branch.commit_id}")
```

#### `delete_branch(repository, branch)`

Delete a branch.

**Parameters:**
- `repository` (str): Repository name
- `branch` (str): Branch name

**Raises:**
- `NotFoundException`: Branch not found
- `ValidationException`: Cannot delete protected branch

**Example:**
```python
branches_api.delete_branch(
    repository="my-repo",
    branch="old-feature"
)
print("Branch deleted successfully")
```

#### `diff_branch(repository, branch, after=None, prefix=None, delimiter=None, amount=None)`

Show changes between branch and its source.

**Parameters:**
- `repository` (str): Repository name
- `branch` (str): Branch name
- `after` (Optional[str]): Return results after this path
- `prefix` (Optional[str]): Filter by path prefix
- `delimiter` (Optional[str]): Path delimiter for grouping
- `amount` (Optional[int]): Maximum number of results

**Returns:**
- `DiffList`: List of changes

**Example:**
```python
diff = branches_api.diff_branch(
    repository="my-repo",
    branch="feature-branch",
    prefix="data/"
)

for change in diff.results:
    print(f"{change.type}: {change.path}")
```

### ObjectsApi

Manage objects within repositories.

#### `list_objects(repository, ref, prefix=None, after=None, delimiter=None, amount=None)`

List objects in a repository reference.

**Parameters:**
- `repository` (str): Repository name
- `ref` (str): Branch name, tag, or commit ID
- `prefix` (Optional[str]): Filter objects by path prefix
- `after` (Optional[str]): Return results after this path
- `delimiter` (Optional[str]): Path delimiter for grouping
- `amount` (Optional[int]): Maximum number of results to return

**Returns:**
- `ObjectStatsList`: List of objects with pagination info

**Example:**
```python
objects_api = lakefs_sdk.ObjectsApi(api_client)

# List all objects
objects = objects_api.list_objects(
    repository="my-repo",
    ref="main"
)

for obj in objects.results:
    print(f"Object: {obj.path} ({obj.size_bytes} bytes)")

# List objects with prefix
data_objects = objects_api.list_objects(
    repository="my-repo",
    ref="main",
    prefix="data/",
    amount=100
)
```

#### `get_object(repository, ref, path, range=None, if_none_match=None, presign=None)`

Download object content.

**Parameters:**
- `repository` (str): Repository name
- `ref` (str): Branch name, tag, or commit ID
- `path` (str): Object path
- `range` (Optional[str]): Byte range (e.g., "bytes=0-1023")
- `if_none_match` (Optional[str]): ETag for conditional requests
- `presign` (Optional[bool]): Return presigned URL instead of content

**Returns:**
- `bytes`: Object content (or presigned URL if presign=True)

**Example:**
```python
# Download object content
content = objects_api.get_object(
    repository="my-repo",
    ref="main",
    path="data/file.txt"
)
print(content.decode('utf-8'))

# Get presigned URL
presigned_url = objects_api.get_object(
    repository="my-repo",
    ref="main",
    path="data/file.txt",
    presign=True
)
```

#### `upload_object(repository, branch, path, content=None, if_none_match=None, storage_class=None)`

Upload object to a branch.

**Parameters:**
- `repository` (str): Repository name
- `branch` (str): Branch name
- `path` (str): Object path
- `content` (Optional[bytes]): Object content
- `if_none_match` (Optional[str]): ETag for conditional upload
- `storage_class` (Optional[str]): Storage class for the object

**Returns:**
- `ObjectStats`: Uploaded object metadata

**Example:**
```python
# Upload text content
content = "Hello, lakeFS!".encode('utf-8')
stats = objects_api.upload_object(
    repository="my-repo",
    branch="main",
    path="data/greeting.txt",
    content=content
)
print(f"Uploaded: {stats.path} ({stats.size_bytes} bytes)")

# Upload with storage class
stats = objects_api.upload_object(
    repository="my-repo",
    branch="main",
    path="archive/old-data.csv",
    content=csv_data,
    storage_class="GLACIER"
)
```

#### `stat_object(repository, ref, path, user_metadata=None, presign=None)`

Get object metadata.

**Parameters:**
- `repository` (str): Repository name
- `ref` (str): Branch name, tag, or commit ID
- `path` (str): Object path
- `user_metadata` (Optional[bool]): Include user metadata
- `presign` (Optional[bool]): Include presigned URL

**Returns:**
- `ObjectStats`: Object metadata

**Example:**
```python
stats = objects_api.stat_object(
    repository="my-repo",
    ref="main",
    path="data/file.txt",
    user_metadata=True
)

print(f"Size: {stats.size_bytes}")
print(f"Modified: {stats.mtime}")
print(f"Checksum: {stats.checksum}")
print(f"Metadata: {stats.metadata}")
```

#### `delete_object(repository, branch, path)`

Delete an object from a branch.

**Parameters:**
- `repository` (str): Repository name
- `branch` (str): Branch name
- `path` (str): Object path

**Example:**
```python
objects_api.delete_object(
    repository="my-repo",
    branch="main",
    path="data/old-file.txt"
)
print("Object deleted successfully")
```

#### `copy_object(repository, branch, dest_path, object_copy_creation)`

Copy an object within or between repositories.

**Parameters:**
- `repository` (str): Destination repository name
- `branch` (str): Destination branch name
- `dest_path` (str): Destination object path
- `object_copy_creation` (ObjectCopyCreation): Copy configuration

**Returns:**
- `ObjectStats`: Copied object metadata

**Example:**
```python
from lakefs_sdk.models import ObjectCopyCreation

copy_config = ObjectCopyCreation(
    src_path="data/source.txt",
    src_ref="main"
)

copied_stats = objects_api.copy_object(
    repository="my-repo",
    branch="feature-branch",
    dest_path="data/copied.txt",
    object_copy_creation=copy_config
)
```

### CommitsApi

Manage commits and commit operations.

#### `commit(repository, branch, commit_creation)`

Create a commit on a branch.

**Parameters:**
- `repository` (str): Repository name
- `branch` (str): Branch name
- `commit_creation` (CommitCreation): Commit configuration

**Returns:**
- `Commit`: Created commit object

**Raises:**
- `NotFoundException`: Repository or branch not found
- `ConflictException`: No changes to commit

**Example:**
```python
from lakefs_sdk.models import CommitCreation

commits_api = lakefs_sdk.CommitsApi(api_client)

commit_creation = CommitCreation(
    message="Add new analytics data",
    metadata={
        "author": "data-team",
        "source": "analytics-pipeline"
    }
)

commit = commits_api.commit(
    repository="my-repo",
    branch="main",
    commit_creation=commit_creation
)
print(f"Created commit: {commit.id}")
```

#### `get_commit(repository, commit_id)`

Get commit details.

**Parameters:**
- `repository` (str): Repository name
- `commit_id` (str): Commit ID

**Returns:**
- `Commit`: Commit object

**Raises:**
- `NotFoundException`: Commit not found

**Example:**
```python
commit = commits_api.get_commit(
    repository="my-repo",
    commit_id="c7a632d0a7c4c9b5e8f1a2b3c4d5e6f7g8h9i0j1"
)
print(f"Commit message: {commit.message}")
print(f"Author: {commit.committer}")
print(f"Date: {commit.creation_date}")
```

### RefsApi

Manage references (branches, tags, commits) and operations between them.

#### `diff_refs(repository, left_ref, right_ref, after=None, prefix=None, delimiter=None, amount=None)`

Compare two references and show differences.

**Parameters:**
- `repository` (str): Repository name
- `left_ref` (str): Left reference (branch, tag, or commit ID)
- `right_ref` (str): Right reference (branch, tag, or commit ID)
- `after` (Optional[str]): Return results after this path
- `prefix` (Optional[str]): Filter by path prefix
- `delimiter` (Optional[str]): Path delimiter for grouping
- `amount` (Optional[int]): Maximum number of results

**Returns:**
- `DiffList`: List of differences between references

**Example:**
```python
refs_api = lakefs_sdk.RefsApi(api_client)

# Compare main branch with feature branch
diff = refs_api.diff_refs(
    repository="my-repo",
    left_ref="main",
    right_ref="feature-branch"
)

for change in diff.results:
    print(f"{change.type}: {change.path}")
    if change.type == "changed":
        print(f"  Size changed: {change.size_bytes}")
```

#### `merge_into_branch(repository, source_ref, destination_branch, merge=None)`

Merge one reference into a branch.

**Parameters:**
- `repository` (str): Repository name
- `source_ref` (str): Source reference to merge from
- `destination_branch` (str): Destination branch to merge into
- `merge` (Optional[Merge]): Merge configuration

**Returns:**
- `MergeResult`: Result of the merge operation

**Raises:**
- `ConflictException`: Merge conflicts detected
- `NotFoundException`: Reference not found

**Example:**
```python
from lakefs_sdk.models import Merge

merge_config = Merge(
    message="Merge feature-analytics into main",
    metadata={
        "merger": "data-team",
        "review_id": "PR-123"
    }
)

result = refs_api.merge_into_branch(
    repository="my-repo",
    source_ref="feature-analytics",
    destination_branch="main",
    merge=merge_config
)

print(f"Merge result: {result.reference}")
print(f"Summary: {result.summary}")
```

#### `log_commits(repository, ref, after=None, amount=None, objects=None, prefixes=None, limit=None, first_parent=None, since=None, until=None)`

Get commit history for a reference.

**Parameters:**
- `repository` (str): Repository name
- `ref` (str): Reference (branch, tag, or commit ID)
- `after` (Optional[str]): Return commits after this commit ID
- `amount` (Optional[int]): Maximum number of commits to return
- `objects` (Optional[List[str]]): Filter by specific object paths
- `prefixes` (Optional[List[str]]): Filter by path prefixes
- `limit` (Optional[bool]): Limit to commits affecting specified paths
- `first_parent` (Optional[bool]): Follow only first parent in merge commits
- `since` (Optional[datetime]): Show commits since this date
- `until` (Optional[datetime]): Show commits until this date

**Returns:**
- `CommitList`: List of commits with pagination info

**Example:**
```python
# Get recent commits
commits = refs_api.log_commits(
    repository="my-repo",
    ref="main",
    amount=10
)

for commit in commits.results:
    print(f"{commit.id[:8]}: {commit.message}")
    print(f"  Author: {commit.committer}")

# Get commits affecting specific paths
data_commits = refs_api.log_commits(
    repository="my-repo",
    ref="main",
    prefixes=["data/"],
    limit=True
)
```

### TagsApi

Manage tags for specific commits.

#### `list_tags(repository, prefix=None, after=None, amount=None)`

List tags in a repository.

**Parameters:**
- `repository` (str): Repository name
- `prefix` (Optional[str]): Filter tags by name prefix
- `after` (Optional[str]): Return results after this tag name
- `amount` (Optional[int]): Maximum number of results to return

**Returns:**
- `TagList`: List of tags with pagination info

**Example:**
```python
tags_api = lakefs_sdk.TagsApi(api_client)

# List all tags
tags = tags_api.list_tags(repository="my-repo")
for tag in tags.results:
    print(f"Tag: {tag.id} -> {tag.commit_id}")

# List version tags
version_tags = tags_api.list_tags(
    repository="my-repo",
    prefix="v"
)
```

#### `create_tag(repository, tag_creation)`

Create a new tag.

**Parameters:**
- `repository` (str): Repository name
- `tag_creation` (TagCreation): Tag configuration

**Returns:**
- `Ref`: Created tag object

**Raises:**
- `ConflictException`: Tag already exists
- `NotFoundException`: Target reference not found

**Example:**
```python
from lakefs_sdk.models import TagCreation

tag_creation = TagCreation(
    id="v1.2.0",
    ref="main"  # Branch, tag, or commit ID
)

tag = tags_api.create_tag(
    repository="my-repo",
    tag_creation=tag_creation
)
print(f"Created tag: {tag.id} -> {tag.commit_id}")
```

#### `get_tag(repository, tag)`

Get tag details.

**Parameters:**
- `repository` (str): Repository name
- `tag` (str): Tag name

**Returns:**
- `Ref`: Tag object

**Raises:**
- `NotFoundException`: Tag not found

**Example:**
```python
tag = tags_api.get_tag(
    repository="my-repo",
    tag="v1.0.0"
)
print(f"Tag {tag.id} points to commit: {tag.commit_id}")
```

#### `delete_tag(repository, tag)`

Delete a tag.

**Parameters:**
- `repository` (str): Repository name
- `tag` (str): Tag name

**Raises:**
- `NotFoundException`: Tag not found

**Example:**
```python
tags_api.delete_tag(
    repository="my-repo",
    tag="v0.9.0-beta"
)
print("Tag deleted successfully")
```

### AuthApi

Manage authentication and authorization.

#### `get_current_user()`

Get information about the current authenticated user.

**Returns:**
- `CurrentUser`: Current user information

**Example:**
```python
auth_api = lakefs_sdk.AuthApi(api_client)

user = auth_api.get_current_user()
print(f"Current user: {user.user.id}")
print(f"Email: {user.user.email}")
```

#### `list_users(prefix=None, after=None, amount=None)`

List users (requires admin privileges).

**Parameters:**
- `prefix` (Optional[str]): Filter users by ID prefix
- `after` (Optional[str]): Return results after this user ID
- `amount` (Optional[int]): Maximum number of results to return

**Returns:**
- `UserList`: List of users with pagination info

**Example:**
```python
users = auth_api.list_users()
for user in users.results:
    print(f"User: {user.id} ({user.email})")
```

## Data Models

The Generated SDK includes comprehensive data models for all API objects. Here are the most commonly used models:

### Repository

Represents a lakeFS repository.

```python
from lakefs_sdk.models import Repository

# Repository properties
repo = Repository(
    id="my-data-lake",
    creation_date=1640995200,  # Unix timestamp
    default_branch="main",
    storage_namespace="s3://my-bucket/repos/my-data-lake",
    read_only=False
)

# Access properties
print(f"Repository ID: {repo.id}")
print(f"Default branch: {repo.default_branch}")
print(f"Storage: {repo.storage_namespace}")
```

### RepositoryCreation

Configuration for creating new repositories.

```python
from lakefs_sdk.models import RepositoryCreation

repo_creation = RepositoryCreation(
    name="new-repo",
    storage_namespace="s3://bucket/path",
    default_branch="main",  # Optional, defaults to "main"
    sample_data=False       # Optional, include sample data
)
```

### Ref (Branch/Tag)

Represents a branch or tag reference.

```python
from lakefs_sdk.models import Ref

# Branch or tag reference
ref = Ref(
    id="feature-branch",
    commit_id="c7a632d0a7c4c9b5e8f1a2b3c4d5e6f7g8h9i0j1"
)

print(f"Reference: {ref.id}")
print(f"Points to commit: {ref.commit_id}")
```

### BranchCreation

Configuration for creating new branches.

```python
from lakefs_sdk.models import BranchCreation

branch_creation = BranchCreation(
    name="feature-analytics",
    source="main"  # Source branch, tag, or commit ID
)
```

### ObjectStats

Object metadata and statistics.

```python
from lakefs_sdk.models import ObjectStats

# Object metadata
stats = ObjectStats(
    path="data/users.csv",
    physical_address="s3://bucket/data/abc123.csv",
    checksum="d41d8cd98f00b204e9800998ecf8427e",
    size_bytes=2048,
    mtime=1640995200,
    metadata={"content-type": "text/csv", "author": "data-team"},
    content_type="text/csv"
)

# Access properties
print(f"Path: {stats.path}")
print(f"Size: {stats.size_bytes} bytes")
print(f"Checksum: {stats.checksum}")
print(f"Metadata: {stats.metadata}")
```

### Commit

Represents a commit in the repository history.

```python
from lakefs_sdk.models import Commit

commit = Commit(
    id="c7a632d0a7c4c9b5e8f1a2b3c4d5e6f7g8h9i0j1",
    parents=["b6a531c0a6c3c8a4d7e0a1b2c3d4e5f6g7h8i9j0"],
    committer="user@example.com",
    message="Add new analytics data",
    creation_date=1640995200,
    metadata={"author": "data-team", "pipeline": "analytics-v2"}
)

print(f"Commit ID: {commit.id}")
print(f"Message: {commit.message}")
print(f"Author: {commit.committer}")
```

### CommitCreation

Configuration for creating new commits.

```python
from lakefs_sdk.models import CommitCreation

commit_creation = CommitCreation(
    message="Update user analytics data",
    metadata={
        "author": "analytics-team",
        "source": "daily-pipeline",
        "version": "1.2.0"
    }
)
```

### Diff

Represents a difference between two references.

```python
from lakefs_sdk.models import Diff

# Diff entry
diff_entry = Diff(
    type="changed",  # "added", "removed", "changed", "conflict"
    path="data/users.csv",
    path_type="object",  # "object" or "common_prefix"
    size_bytes=2048
)

print(f"Change type: {diff_entry.type}")
print(f"Path: {diff_entry.path}")
```

### Merge

Configuration for merge operations.

```python
from lakefs_sdk.models import Merge

merge_config = Merge(
    message="Merge feature-analytics into main",
    metadata={
        "merger": "data-team",
        "review_id": "PR-123",
        "approved_by": "team-lead"
    },
    strategy="dest-wins",  # Optional: merge strategy
    force=False           # Optional: force merge
)
```

### TagCreation

Configuration for creating new tags.

```python
from lakefs_sdk.models import TagCreation

tag_creation = TagCreation(
    id="v1.2.0",
    ref="main"  # Branch, tag, or commit ID to tag
)
```

## Error Handling

The Generated SDK uses exceptions to handle API errors. All API exceptions inherit from `ApiException`.

### Exception Hierarchy

```python
from lakefs_sdk.rest import ApiException

# Base exception for all API errors
try:
    result = api_call()
except ApiException as e:
    print(f"API Error: {e.status} - {e.reason}")
    print(f"Response body: {e.body}")
```

### Common Exception Types

#### HTTP Status Code Based Handling

```python
from lakefs_sdk.rest import ApiException

try:
    repo = repositories_api.get_repository("nonexistent-repo")
except ApiException as e:
    if e.status == 404:
        print("Repository not found")
    elif e.status == 401:
        print("Authentication failed - check credentials")
    elif e.status == 403:
        print("Access denied - insufficient permissions")
    elif e.status == 409:
        print("Conflict - resource already exists or is in use")
    elif e.status == 422:
        print("Validation error - invalid input")
    elif e.status >= 500:
        print("Server error - try again later")
    else:
        print(f"Unexpected error: {e.status} - {e.reason}")
```

#### Detailed Error Information

```python
import json
from lakefs_sdk.rest import ApiException

try:
    # API operation that might fail
    result = repositories_api.create_repository(invalid_config)
except ApiException as e:
    print(f"HTTP Status: {e.status}")
    print(f"Reason: {e.reason}")
    
    # Parse error details from response body
    try:
        error_details = json.loads(e.body)
        print(f"Error message: {error_details.get('message', 'Unknown error')}")
        if 'error_code' in error_details:
            print(f"Error code: {error_details['error_code']}")
    except (json.JSONDecodeError, AttributeError):
        print(f"Raw error body: {e.body}")
```

#### Retry Logic with Exponential Backoff

```python
import time
import random
from lakefs_sdk.rest import ApiException

def api_call_with_retry(api_func, max_retries=3, base_delay=1):
    """Execute API call with exponential backoff retry logic."""
    for attempt in range(max_retries + 1):
        try:
            return api_func()
        except ApiException as e:
            if e.status >= 500 and attempt < max_retries:
                # Server error - retry with exponential backoff
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"Server error, retrying in {delay:.2f}s (attempt {attempt + 1})")
                time.sleep(delay)
            else:
                # Client error or max retries reached
                raise

# Usage example
try:
    result = api_call_with_retry(
        lambda: repositories_api.list_repositories()
    )
except ApiException as e:
    print(f"Failed after retries: {e.status} - {e.reason}")
```

## Pagination

Many API endpoints return paginated results. The Generated SDK provides consistent pagination patterns across all list operations.

### Pagination Response Structure

All paginated responses include a `pagination` object:

```python
response = repositories_api.list_repositories(amount=10)

# Access results
for repo in response.results:
    print(f"Repository: {repo.id}")

# Check pagination info
pagination = response.pagination
print(f"Has more results: {pagination.has_more}")
print(f"Next offset: {pagination.next_offset}")
print(f"Results count: {pagination.results}")
print(f"Max per page: {pagination.max_per_page}")
```

### Manual Pagination

```python
def list_repositories_page_by_page():
    """List repositories one page at a time."""
    after = ""
    page_num = 1
    
    while True:
        print(f"Fetching page {page_num}...")
        
        response = repositories_api.list_repositories(
            after=after,
            amount=50  # Page size
        )
        
        # Process current page
        for repo in response.results:
            print(f"  Repository: {repo.id}")
        
        # Check if more pages exist
        if not response.pagination.has_more:
            break
            
        after = response.pagination.next_offset
        page_num += 1
    
    print(f"Processed {page_num} pages total")
```

### Automatic Pagination Helper

```python
def list_all_items(list_func, **kwargs):
    """Generic helper to fetch all items from a paginated API."""
    all_items = []
    after = ""
    
    while True:
        # Call the list function with pagination parameters
        response = list_func(after=after, amount=1000, **kwargs)
        
        # Add items from current page
        all_items.extend(response.results)
        
        # Check if more pages exist
        if not response.pagination.has_more:
            break
            
        after = response.pagination.next_offset
    
    return all_items

# Usage examples
all_repos = list_all_items(repositories_api.list_repositories)
all_branches = list_all_items(
    branches_api.list_branches,
    repository="my-repo"
)
all_objects = list_all_items(
    objects_api.list_objects,
    repository="my-repo",
    ref="main",
    prefix="data/"
)
```

### Filtering with Pagination

```python
def find_repositories_by_pattern(pattern):
    """Find repositories matching a pattern across all pages."""
    matching_repos = []
    after = ""
    
    while True:
        response = repositories_api.list_repositories(
            after=after,
            amount=100
        )
        
        # Filter results on current page
        for repo in response.results:
            if pattern in repo.id:
                matching_repos.append(repo)
        
        if not response.pagination.has_more:
            break
            
        after = response.pagination.next_offset
    
    return matching_repos

# Find all repositories containing "data"
data_repos = find_repositories_by_pattern("data")
```

### Performance Considerations

```python
# Optimize page size based on use case
def list_with_optimal_pagination(list_func, **kwargs):
    """Use larger page sizes for better performance."""
    all_items = []
    after = ""
    
    # Use maximum page size for fewer API calls
    page_size = 1000  # Adjust based on API limits
    
    while True:
        response = list_func(
            after=after,
            amount=page_size,
            **kwargs
        )
        
        all_items.extend(response.results)
        
        if not response.pagination.has_more:
            break
            
        after = response.pagination.next_offset
        
        # Optional: Add progress indication for large datasets
        if len(all_items) % 10000 == 0:
            print(f"Fetched {len(all_items)} items so far...")
    
    return all_items
```

## Next Steps

- See [usage examples](examples.md) for practical implementations
- Learn about [direct access](direct-access.md) from High-Level SDK
- Review the [complete API documentation](https://pydocs-sdk.lakefs.io)