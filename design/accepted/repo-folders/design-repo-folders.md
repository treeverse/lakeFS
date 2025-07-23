# Technical Design: Folders for Repositories

## Background

This document provides the technical design for implementing folders for repositories in lakeFS. It is based on the
requirements outlined in the [PRD: Folders for Repositories](./prd-repo-folders.md). The goal is to introduce a
folder-like structure for organizing repositories, improving discoverability and manageability, especially for users
with a large number of repositories.

## Guiding Principles

The implementation will adhere to the following principles:

- **Folders as Entities:** Folders are distinct entities within the KV store. This allows for empty folders and
  provides performant listing of the folder hierarchy.
- **Minimal Core Impact:** The changes to the core `Repository` entity will be minimal. A repository will have a new
  attribute to store its folder path, and a separate index will be maintained for efficient lookups.
- **UI-Focused:** The primary consumer of this feature is the UI. The impact on backend services and `lakectl` will be
  minimized for V1.
- **KV-Optimized:** The design will leverage the KV store for efficient querying of folders and their contents, using
  explicit folder entries and secondary indexes.

## Data Model

### Folder Representation

A folder is a distinct entity that represents a directory. This allows for the existence of empty folders and provides
better performance for listing folder structures, as it avoids scanning repository data. A folder is stored as its own
entry in the KV store.

### Repository Changes

The `Repository` entity in our data model will be extended with a new, optional attribute:

- `folder` (string): The full path of the folder containing the repository. For example, `team-a/project-x`. If a
  repository is at the root level, this attribute will be empty or null.

## API Design

The following API endpoints will be created or modified to support folder management.

### New Endpoints

#### List Entries (Folders and Repositories)

A new endpoint will be created to list the contents of a given folder, which can include both sub-folders and
repositories.

- **Endpoint:** `GET /folders`
- **Description:** Lists the direct sub-folders and repositories of a given folder path.
- **Query Parameters:**
    - `path` (string, optional): The folder path to list. If not provided, lists the root.
- **Response (200 OK):**
  ```json
  {
    "path": "team-a",
    "results": [
      { "id": "project-x", "kind": "folder" },
      { "id": "my-cool-repo", "kind": "repo" }
    ],
    "pagination": {
      "has_more": false,
      "next_offset": ""
    }
  }
  ```
- **Implementation Notes:** This endpoint will query the repositories, group them by their `folder` attribute, and
  extract the folder names and repository names for the given path.

#### Create Folder

- **Endpoint:** `POST /folders`
- **Description:** Creates a new, empty folder.
- **Request Body:**
  ```json
  { "path": "team-a/project-x" }
  ```
- **Response (201 Created)**
- **Implementation Notes:** This will create a `Folder` entry in the KV store. If any parent folders in the path do not
  exist, they will be created as well. For example, creating `team-a/project-x` will also create `team-a` if it doesn't
  exist.

#### Delete Empty Folder

- **Endpoint:** `DELETE /folders/*path`
- **Description:** Deletes an empty folder path. The operation will fail if the folder contains any repositories or
  sub-folders.
- **Response (204 No Content)**
- **Implementation Notes:** The operation will fail if the folder contains any repositories or
  sub-folders. This is checked by scanning for repositories in `repo_by_folder/{path}/` and for sub-folders with a
  prefix scan on `folders/{path}/`. If the folder is empty, its corresponding entry in the KV store will be deleted.

#### Move a Repository to a Folder

A new endpoint to update the folder of a repository.

- **Endpoint:** `PATCH /repositories/{repository}/folder`
- **Description:** Assigns or changes the folder for a repository.
- **Request Body:**
  ```json
  { "folder": "team-a/project-x" }
  ```
- **Response (204 No Content)**
- **Implementation Notes:** This will be a simple update to the `folder` attribute of the specified repository.

### Modified Endpoints

#### List Repositories

The existing `GET /repositories` endpoint will be updated to support filtering by folder.

- **Endpoint:** `GET /repositories`
- **New Query Parameter:**
    - `folder` (string, optional): The folder path to filter by.
- **Implementation Notes:** The response will be filtered based on the `folder` query parameter. The `Repository`
  object in the response will also include the `folder` attribute.

## Backend Implementation

### KV Store Data Model

The data model will consist of two main types of entries in the KV store: `Folder` entries and `Repository` entries.

#### Folder Entries

A `Folder` is a distinct entity in the KV store representing a directory.

- **Key Format:** `folders/{full_path}` (e.g., `folders/team-a/project-x`)
- **Value:** Can be empty or hold folder-specific metadata in the future.

This structure allows for efficient listing of the folder hierarchy using prefix scans on `folders/`.

#### Repository Entries

The `Repository` entity in the KV store will be updated to support folder assignment.

- **Repository Data:** The `Repository` entity will be extended with an optional `folder` attribute (e.g.,
  `"folder": "team-a/project-x"`).
- **Index for Repositories by Folder:** To efficiently list repositories within a specific folder, a secondary index
  will be maintained.
    - **Key Format:** `repo_by_folder/{folder_path}/{repository_name}`
    - **Value:** The repository ID.

This dual structure of explicit folder entries and an index for repositories allows for performant listing of folder
contents (`GET /folders`), which needs to return both sub-folders and repositories.

### Service Layer

The `graveler` service will be updated to manage both `Folder` and `Repository` entities.

- The `CreateRepository` function will be updated to accept an optional `folder` parameter. When a repository is
  created, it will also ensure that all parent folders in its path exist, creating them if necessary. It will also add
  an entry to the `repo_by_folder` index.
- The `ListRepositories` function will use the `repo_by_folder` index to filter by folder efficiently.
- A new function `UpdateRepositoryFolder` will update the repository's `folder` attribute and move its entry in the
  `repo_by_folder` index. It will not automatically delete the old folder if it becomes empty.
- The `DeleteRepository` function must be updated to remove the repository's entry from the `repo_by_folder` index.
- The `GET /folders` endpoint will perform two KV scans:
    1. A scan for sub-folders using the `folders/` prefix.
    2. A scan for repositories using the `repo_by_folder/` prefix.
       The results from both scans will be combined to present a unified view of the folder's contents.

## UI/UX Implementation

The UI will be updated to provide a hierarchical view of repositories and folders.

- **Repository List Page:** This page will be the main interface for interacting with folders. It will display a list
  of folders and repositories at the current path.
- **Breadcrumbs:** A breadcrumb navigation bar will show the current folder path, allowing users to navigate up the
  hierarchy.
- **Folder Buttons:** Buttons for creating and deleting folders will be added to the UI. The create folder button
  will prompt the user for a folder name, while the delete folder button will only be enabled if the folder is empty.
- **Move Folder Button:** A button to move repositories will be added. When clicked, it will open a dialog to select a
  destination folder, which will display the current folders structure for easy navigation.
- **Move Repository Dialog:** A modal or dialog will be created to allow users to select a destination folder when
  moving a repository.
- **State Management:** For simplicity, the current folder path will be managed in the UI state. It won't be stored
  in the URL or browser history for V1, but this can be added later if needed.

## Tasks and Deliverables

The work can be broken down into the following tasks:

### Phase 1: Backend

1. **KV Store Migration:**
    -   [ ] Create a migration process to add the optional `folder` attribute to existing repositories.
    -   [ ] The migration should also build the `repo_by_folder` index for all existing repositories.
2. **API Endpoints:**
    -   [ ] Implement `GET /folders` to list folder contents.
    -   [ ] Implement `POST /folders` to create a folder.
    -   [ ] Implement `DELETE /folders/*path` to delete an empty folder.
    -   [ ] Implement `PATCH /repositories/{repository}/folder` to move a repository.
    -   [ ] Update `GET /repositories` to support filtering by folder.
3. **Service Layer:**
    -   [ ] Update the `graveler` service to manage `Folder` entities and the `folder` attribute on `Repository`
        entities.
    -   [ ] Implement the business logic for the new endpoints, including all index management.
4. **Testing:**
    -   [ ] Write unit and integration tests for the new API endpoints and service logic.

### Phase 2: Frontend (UI)

1. **Repository List View:**
    -   [ ] Update the repository list to display both folders and repositories.
    -   [ ] Implement navigation into and out of folders.
    -   [ ] Add breadcrumbs for folder navigation.
2. **Folder Management UI:**
    -   [ ] Add UI elements for creating and deleting folders.
    -   [ ] Implement the "Move Repository" feature with a folder selection dialog.
3. **State Management:**
    -   [ ] Update the API client to interact with the new folder endpoints.
    -   [ ] Manage the current folder path in the UI state.
4. **Testing:**
    -   [ ] Write E2E tests for folder navigation and management.

### Phase 3: Documentation

1. **API Documentation:**
    -   [ ] Update the OpenAPI/Swagger documentation for the new and modified endpoints.
2. **User Documentation:**
    -   [ ] Create a guide on how to use the new repository folders feature.

## Out of Scope (for V1)

As defined in the PRD:

- Renaming or moving folders directly.
- Folder-level permissions.
- `lakectl` support for folder management.

## Risks and Mitigations

- **Performance:** Listing folders in a system with tens of thousands of repositories could be slow.
    - **Mitigation:** The proposed KV store index provides efficient prefix scans, which should be performant. We will
      benchmark this during development.
- **Data Migration:** The migration of existing repositories needs to be handled carefully.
    - **Mitigation:** All existing repositories will initially be considered to be in the root folder. A one-time
      migration process will be required to populate the `repo_by_folder` index for these repositories. For large
      installations, this could be a lengthy operation.
