---
title: Generated Python SDK (lakefs_sdk)
description: Understand and use the Generated Python SDK for direct access to the lakeFS API.
parent: Python Integration
grand_parent: Integrations
---

# Generated Python SDK (`lakefs_sdk`)

{% raw %}{% include toc.html %}{% endraw %}

## Overview

The Generated Python SDK, `lakefs_sdk`, provides direct, low-level access to the complete lakeFS REST API. It is automatically generated from the lakeFS OpenAPI specification, ensuring that all API endpoints and their functionalities are available to Python developers.

While the [High-Level Python SDK (`lakefs`)](./high_level_sdk.md) is recommended for most common tasks due to its more Pythonic and abstracted interface, the Generated SDK is valuable for specific scenarios.

### When to Use the Generated SDK

*   **Accessing bleeding-edge features**: If a new API endpoint or parameter has been added to lakeFS but not yet incorporated into the High-Level SDK, the Generated SDK allows you to use it immediately.
*   **Fine-grained control**: When you need precise control over API request parameters or want to work directly with the raw API response structures.
*   **Specific or less common operations**: For operations that are not frequently used and thus might not have a dedicated abstraction in the High-Level SDK.
*   **Building custom abstractions**: If you are developing tools or libraries on top of lakeFS and prefer to work directly with the API layer.

### Key Concepts

*   **API Client (`ApiClient`)**: Manages the connection and authentication with your lakeFS server. You instantiate this with a `Configuration` object.
*   **API Classes (e.g., `RepositoriesApi`, `ObjectsApi`)**: Each class corresponds to a group of related API endpoints (e.g., all repository operations). You instantiate these with an `ApiClient` instance.
*   **Model Classes (e.g., `RepositoryCreation`, `CommitCreation`)**: These classes define the structure of data sent to or received from the API. You'll use them to construct request bodies and interpret responses.
*   **Exception Handling (`ApiException`, `NotFoundException`)**: API errors are raised as specific exceptions, providing details like status code and error messages.

### Installation

Install the Generated Python SDK using pip:

```shell
pip install lakefs-sdk
```

### Initializing the Client

To use the Generated SDK, you first create a `Configuration` object with your lakeFS server details and credentials. Then, you instantiate an `ApiClient` with this configuration. Specific API interaction classes (like `RepositoriesApi`) are then initialized with the `ApiClient`.

```python
from lakefs_sdk import Configuration, ApiClient
from lakefs_sdk.api import repositories_api # Example API class

# --- Configuration ---
# IMPORTANT: Replace these placeholders with your actual lakeFS endpoint and credentials.
LAKEFS_ENDPOINT = "http://your-lakefs-endpoint:8000"  # e.g., "http://localhost:8000"
LAKEFS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY_ID"
LAKEFS_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"

configuration = Configuration(
    host=f"{LAKEFS_ENDPOINT}/api/v1", # Note: host includes the /api/v1 path
    username=LAKEFS_ACCESS_KEY_ID,
    password=LAKEFS_SECRET_ACCESS_KEY
)

# Example of creating an API client and an API instance
# This is typically done within a 'with' block for resource management
# with ApiClient(configuration) as api_client:
#     repo_api = repositories_api.RepositoriesApi(api_client)
    # Use repo_api to make calls
```

### API Reference

The Generated Python SDK is extensive. For detailed information on all available API classes, methods, models, and parameters, refer to the **Generated Python SDK Documentation**:
[https://pydocs-sdk.lakefs.io](https://pydocs-sdk.lakefs.io)

This documentation is the definitive source for understanding how to use every part of the lakeFS API through Python.

## Tutorials / User Guides

This section provides step-by-step examples for common tasks using the Generated SDK.

### Example Workflow: Repository, Branch, Upload, Commit, List

This comprehensive example demonstrates a common workflow:
1.  Creating a repository.
2.  Creating a new branch.
3.  Uploading a file to the new branch.
4.  Committing the changes.
5.  Listing objects in the branch.

```python
from pprint import pprint
import lakefs_sdk
from lakefs_sdk.api import repositories_api, branches_api, objects_api, commits_api 
from lakefs_sdk.models import RepositoryCreation, BranchCreation, CommitCreation # ObjectStats is implicitly used by api
from lakefs_sdk.configuration import Configuration
from lakefs_sdk.exceptions import ApiException, NotFoundException
import os

# --- Configuration (ensure these are set as shown in Initialization) ---
# IMPORTANT: Replace these placeholders with your actual lakeFS endpoint and credentials.
LAKEFS_ENDPOINT = "http://your-lakefs-endpoint:8000" 
LAKEFS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY_ID"
LAKEFS_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"
# IMPORTANT: Update this storage_namespace_base to your actual S3/GCS/Azure base path.
# e.g., "s3://my-lakefs-storage-bucket/repos/" or "gs://my-lakefs-storage-bucket/repos/"
STORAGE_NAMESPACE_BASE = "s3://your-storage-bucket/repos/" 

configuration = Configuration(
    host=f"{LAKEFS_ENDPOINT}/api/v1",
    username=LAKEFS_ACCESS_KEY_ID,
    password=LAKEFS_SECRET_ACCESS_KEY
)

# --- Workflow Variables ---
repo_name = "generated-sdk-workflow-repo"
main_branch_name = "main" # Default branch for repo creation
feature_branch_name = "feat/sdk-upload"
file_in_repo_path = "data/generated_sdk_sample.txt"
local_file_content_data = "Content created by lakeFS Generated SDK."
temp_local_file_for_upload = "temp_generated_sdk_upload.txt"

with lakefs_sdk.ApiClient(configuration) as api_client:
    repo_api = repositories_api.RepositoriesApi(api_client)
    branch_api = branches_api.BranchesApi(api_client)
    objects_api_instance = objects_api.ObjectsApi(api_client)
    commits_api_instance = commits_api.CommitsApi(api_client)

    active_branch_ref = main_branch_name # Start with the main branch

    try:
        # 1. Create Repository
        print(f"--- 1. Creating Repository '{repo_name}' ---")
        repo_creation_payload = RepositoryCreation(
            name=repo_name,
            storage_namespace=f"{STORAGE_NAMESPACE_BASE}{repo_name}/", # Ensure STORAGE_NAMESPACE_BASE is correctly set
            default_branch=main_branch_name,
        )
        try:
            repo_details = repo_api.create_repository(repository_creation=repo_creation_payload)
            print(f"Repository '{repo_details.id}' created.")
        except ApiException as e:
            if e.status == 409: # Conflict
                print(f"Repository '{repo_name}' already exists. Using existing.")
                repo_details = repo_api.get_repository(repository=repo_name) # Get existing repo details
            else:
                print(f"Error creating repository: Status {e.status}, Reason: {e.reason}, Body: {e.body}")
                raise 

        # 2. Create Feature Branch
        print(f"\n--- 2. Creating Branch '{feature_branch_name}' from '{main_branch_name}' ---")
        branch_creation_payload = BranchCreation(name=feature_branch_name, source=main_branch_name)
        try:
            branch_details = branch_api.create_branch(repository=repo_name, branch_creation=branch_creation_payload)
            print(f"Branch '{branch_details.reference}' created.")
            active_branch_ref = branch_details.reference
        except ApiException as e:
            if e.status == 409: # Conflict
                print(f"Branch '{feature_branch_name}' already exists. Using existing.")
                active_branch_ref = feature_branch_name # Assumes it exists with this name, get details if needed
            else:
                print(f"Error creating branch: Status {e.status}, Reason: {e.reason}, Body: {e.body}")
                raise

        # 3. Upload Object to Feature Branch
        print(f"\n--- 3. Uploading object to '{repo_name}/{active_branch_ref}/{file_in_repo_path}' ---")
        with open(temp_local_file_for_upload, "w") as f:
            f.write(local_file_content_data)
        
        try:
            with open(temp_local_file_for_upload, "rb") as file_stream:
                upload_stats = objects_api_instance.upload_object(
                    repository=repo_name,
                    branch=active_branch_ref,
                    path=file_in_repo_path,
                    content=file_stream,
                    content_type="text/plain" # Optional: specify content type
                )
            print(f"Object uploaded. Stats:")
            pprint(upload_stats)
        except ApiException as e:
            print(f"Error uploading object: Status {e.status}, Reason: {e.reason}, Body: {e.body}")
            raise
        finally:
            if os.path.exists(temp_local_file_for_upload):
                os.remove(temp_local_file_for_upload)

        # 4. Commit Changes
        print(f"\n--- 4. Committing changes to branch '{active_branch_ref}' ---")
        commit_creation_payload = CommitCreation(
            message=f"Added {file_in_repo_path}",
            metadata={'author': 'generated_sdk_script', 'type': 'data_upload'}
        )
        try:
            commit_details = commits_api_instance.commit(
                repository=repo_name,
                branch=active_branch_ref,
                commit_creation=commit_creation_payload
            )
            print(f"Commit successful. ID: {commit_details.id}")
        except ApiException as e:
            if e.status == 400 and e.body and "nothing to commit" in str(e.body).lower():
                 print(f"No new changes to commit on branch '{active_branch_ref}'.")
            else:
                print(f"Error committing changes: Status {e.status}, Reason: {e.reason}, Body: {e.body}")
                raise

        # 5. List Objects
        print(f"\n--- 5. Listing objects in '{repo_name}/{active_branch_ref}' under 'data/' ---")
        ls_response = objects_api_instance.list_objects(
            repository=repo_name,
            ref=active_branch_ref,
            prefix="data/",
            amount=10 # Max number of results per page
        )
        print(f"Found {len(ls_response.results)} object(s):")
        for obj_stat in ls_response.results:
            print(f"  - Path: {obj_stat.path}, Size: {obj_stat.size_bytes}, Type: {obj_stat.path_type}")
        if not ls_response.results:
            print("  No objects found with that prefix.")

    except ApiException as e:
        print(f"Workflow API Error: Status {e.status}, Reason: {e.reason}, Body: {e.body}")
    except NotFoundException as e: # Should be caught by ApiException but can be specific if needed
        print(f"Workflow Not Found Error: Status {e.status}, Reason: {e.reason}, Body: {e.body}")
    except Exception as e:
        print(f"An unexpected error occurred during the workflow: {e}")

```

### Example: Listing Repositories with Pagination

This example shows how to list repositories and handle pagination if many repositories exist.

```python
from pprint import pprint
import lakefs_sdk
from lakefs_sdk.api import repositories_api
from lakefs_sdk.configuration import Configuration
from lakefs_sdk.exceptions import ApiException

# --- Configuration (ensure these are set as shown in Initialization) ---
# IMPORTANT: Replace these placeholders with your actual lakeFS endpoint and credentials.
LAKEFS_ENDPOINT = "http://your-lakefs-endpoint:8000" 
LAKEFS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY_ID"
LAKEFS_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"

configuration = Configuration(
    host=f"{LAKEFS_ENDPOINT}/api/v1",
    username=LAKEFS_ACCESS_KEY_ID,
    password=LAKEFS_SECRET_ACCESS_KEY
)

with lakefs_sdk.ApiClient(configuration) as api_client:
    repo_api_instance = repositories_api.RepositoriesApi(api_client) 
    try:
        print("--- Listing Repositories (with pagination handling) ---")
        
        all_repositories = []
        after_cursor = None 
        results_per_page = 10 # Adjust as needed, max 1000

        while True:
            print(f"Fetching page of repositories... (after_cursor: '{after_cursor if after_cursor else 'None'}')")
            repo_list_page = repo_api_instance.list_repositories(
                amount=results_per_page, 
                after=after_cursor
            )
            
            if repo_list_page.results:
                all_repositories.extend(repo_list_page.results)
                print(f"Fetched {len(repo_list_page.results)} repositories this page. (Total collected: {len(all_repositories)})")
            else:
                print("No repositories found on this page.")
            
            if not repo_list_page.pagination.has_more:
                print("No more pages of repositories to fetch.")
                break 
            after_cursor = repo_list_page.pagination.next_offset
            print(f"Next page cursor: {after_cursor}")
        
        print(f"\nTotal repositories found: {len(all_repositories)}")
        for repo_item in all_repositories: 
            print(f"  - ID: {repo_item.id}, Default Branch: {repo_item.default_branch}, Storage: {repo_item.storage_namespace}")

    except ApiException as e:
        print(f"API Error listing repositories: Status {e.status}, Reason: {e.reason}, Body: {e.body}")
    except Exception as e:
        print(f"An unexpected error occurred while listing repositories: {e}")
```

### Error Handling

The Generated SDK primarily uses `lakefs_sdk.exceptions.ApiException` for errors returned by the API. `lakefs_sdk.exceptions.NotFoundException` (a subclass of `ApiException`) is specifically raised for 404 errors.

When an `ApiException` occurs, you can inspect its properties:
*   `e.status`: The HTTP status code (e.g., 400, 401, 403, 404, 409, 500).
*   `e.reason`: The HTTP reason phrase (e.g., "Bad Request", "Not Found").
*   `e.body`: Often contains a JSON string with more detailed error information from the lakeFS server.
*   `e.headers`: The HTTP response headers.

Always wrap your API calls in `try...except` blocks to handle these exceptions:
```python
# Assuming 'configuration' is defined and 'api_client' is created within a 'with' block
# from lakefs_sdk.api import branches_api # Example
# from lakefs_sdk.exceptions import ApiException, NotFoundException

# with lakefs_sdk.ApiClient(configuration) as api_client:
#     branch_api_instance = branches_api.BranchesApi(api_client)
try:
    # Example: Attempt to get a non-existent branch
    # Replace with your actual API call and parameters
    # branch_details = branch_api_instance.get_branch(repository="my-repo", branch="non-existent-branch")
    # print(f"Branch details: {branch_details}") # This line won't be reached if NotFoundException occurs
    print("Placeholder for an actual API call that might fail.") # Placeholder
    # Simulate an error for demonstration if you don't have a live setup:
    # raise NotFoundException(status=404, reason="Not Found Simulation")
    
except NotFoundException as e:
    print(f"Resource not found: {e.reason} (Status: {e.status})")
    # For more details, you can log or inspect e.body, e.g.:
    # print(f"Details: {e.body}") 
except ApiException as e:
    print(f"An API error occurred: {e.reason} (Status: {e.status})")
    # print(f"Details: {e.body}")
except Exception as e: # Catch any other non-API errors
    print(f"A general error occurred: {e}")

```

## Best Practices and Performance

For guidance on optimizing your use of the Python SDKs, choosing the right tools for different scenarios, and general performance considerations, please refer to our comprehensive guide:
[Python SDKs: Best Practices & Performance](./best_practices.md)

---
