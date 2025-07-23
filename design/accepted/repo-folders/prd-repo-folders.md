# PRD: Folders for Repositories

## Background

As the number of repositories in lakeFS grows, a flat list of repositories becomes increasingly difficult to navigate
and manage. Users, especially in larger organizations, need a way to group and organize repositories to reflect their
team structures, projects, or any other logical categorization. This is similar to how folders or groups work in
systems
like GitLab.

This feature request comes from a customer (Entrust) who is struggling with managing a large number of repositories and
needs better discoverability and organization.

## Problem Statement

The current flat-list view of repositories does not scale well for users with a large number of repositories. It's hard
to find specific repositories, understand their context, or manage them efficiently. This leads to a poor user
experience and decreased productivity.

## Goals and Objectives

**Primary Goal:** Improve repository organization and discoverability by introducing a folder-like structure for
repositories.

**Objectives:**

- Allow users to group repositories into logical folders
- Provide a user-friendly interface for managing folders and moving repositories
- Expose folder management capabilities through the API
- Enable easier navigation and searching of repositories within a hierarchical structure

## Guiding Principles

To minimize impact on the existing system, this feature will be implemented with the following principles in mind:

- **Folders are External Metadata:** Folders are an organizational concept, external to the repository entity itself.
  The folder structure is a metadata layer that lives alongside repositories.
- **Repository Awareness:** A repository entity will maintain a reference to its containing folder. This allows for
  efficient folder content listing while keeping the repository's core identity and API unchanged.
- **UI-Focused:** The primary consumer of this feature is the UI, for display and navigation purposes. The impact on
  backend services and command-line tools like `lakectl` should be minimal.
- **KV-Optimized Design:** The implementation leverages KV store characteristics with efficient prefix-based scanning
  and secondary indices for performance.

## User Stories

- As a lakefs administrator, I want to create folders for different teams (e.g., `frontend/`, `backend/`,
  `data-science/`) so that each team can easily find and manage their own repositories
- As a data engineer, I want to organize repositories for different projects I'm working on (e.g., `project-x/`,
  `project-y/`) to keep my workspace tidy
- As a team lead, I want to move existing repositories into a newly created folder for my team without losing any data
  or history
- As a developer, I want to be able to search for repositories within a specific folder to narrow down my search
  results
- As an automation engineer, I want to use the API to create folders and assign new repositories to them as part of our
  CI/CD pipeline
- As a user, I want to rename a folder if our team or project name changes
- As a user, I want to delete an empty folder that is no longer needed

## Requirements

### Functional Requirements

#### Folder Management (UI and API)

**Create Folder:**

- Users should be able to create a new folder from the repositories list view
- Folders can be nested (e.g., `team-a/project-x/`)
- API endpoint to create a folder

**Delete Folder:**

- Users should be able to delete a folder
- A folder can only be deleted if it is empty (contains no repositories or sub-folders)
- API endpoint to delete a folder

**Move Repositories:**

- Users should be able to move one or more repositories from one folder to another, or into the root
- This action updates the folder association for the repository but does not modify the repository itself
- This should be possible via a "Move" action in the UI, perhaps with drag-and-drop support
- API endpoint to update a repository's folder assignment

#### Folder Naming

- Folder names must adhere to the same validation rules as repository names:
    - Between 3 and 63 characters long.
    - Comprised of lowercase letters, numbers, and hyphens (`a-z`, `0-9`, `-`).
    - Start with a letter or a number.
- This will be enforced by both the UI and the API.

### UI/UX Requirements

**Repository List View:**

- The repository list should display both folders and repositories hierarchically
- Folders should be clearly distinguishable from repositories
- Folders may appear first in the list, followed by repositories

**Folder Navigation:**

- Users should be able to navigate into folders to view their contents
- The UI should display all repositories and sub-folders within a folder (only its direct children).
- A breadcrumb navigation should be provided to show the current folder path
- Users should be able to return to the parent folder or root easily

**Search:**

- The search functionality should be enhanced to allow searching within the current folder or globally

### API Requirements

The folder structure is implicitly defined by a `folder` attribute on the Repository entity. This attribute holds the
full path of the folder the repository resides in.

#### Folder and Repository Models

- **Folder**: A folder is not a distinct entity but is represented by a `path` string (e.g., "parent-1/folder-1").
- **Repository**: The Repository entity will have a new optional attribute:
    - `folder` (string, optional): The full path of the folder containing the repository.

#### Endpoints

**1. List Folders**

- `GET /folders`
- **Description**: Lists the direct sub-folders of a given folder path.
- **Query Parameters**:
    - `path` (string, optional): The folder path to list. If not provided, lists the root.
- **Response (200 OK)**:
  ```json
  {
    "path": "team-a",
    "results": [
      { "id": "my-cool-repo", "kind": "repo" },
      { "id": "project-x", "kind": "folder" }
    ]
  }
  ```

**2. Create Folder**

- `POST /folders`
- **Description**: Creates an empty folder.
- **Request Body**:
  ```json
  { "path": "team-a/project-x" }
  ```
- **Response (201 Created)**

**3. Delete Empty Folder**

- `DELETE /folders/*path`
- **Description**: Deletes an empty folder path. The operation will fail if the folder contains any repositories or
  sub-folders.
- **Response (204 No Content)**

**4. Move a Repository to a Folder**

- `PATCH /repositories/{repoName}/folder`
- **Description**: Assigns or changes the folder for a repository.
- **Request Body**:
  ```json
  { "path": "team-a/project-x" }
  ```
- **Response (204 No Content)**

**5. List Repositories (update existing endpoint)**

- `GET /repositories` (Existing)
- **Description**: Lists repositories, with a new option to filter by folder.
- **New Query Parameter**:
    - `folder` (string, optional): The folder path to filter by. If provided, only repositories within this folder will
      be returned. If not provided, repositories from all folders (including the root) will be returned.
- **Response (200 OK)**: The existing `RepositoryList` response. The `Repository` objects within the list will now
  include the optional `folder` attribute.

### Non-Functional Requirements

**Scale:** lakeFS is designed to support tens of thousands of repositories. The folder implementation should not become
a bottleneck and should be able to handle this scale gracefully. The UI must remain responsive even when displaying a
large number of repositories and folders.

**Performance:** Folder operations and repository listing should be performant, even with a large number of folders and
repositories.

**Permissions:** Existing repository permissions should be respected. Folder-level permissions are **out of scope** for
this initial version but should be considered in the design.

## Out of Scope (for V1)

- **Renaming or Moving Folders:** These operations are out of scope for V1. The recommended workaround is to create a
  new folder, move all its content to the new location, and delete the original folder.
- **Folder-level permissions:** Access control will continue to be managed at the repository level
- **`lakectl` support:** The initial version will not include commands for folder management.
