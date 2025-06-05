---
title: ACL Server Implementation
description: Instructions for implementing an ACL server to manage permissions in lakeFS OSS.
nav_order: 1
---

# ACL Server Implementation

{% include toc_2-4.html %}

## Overview

This guide explains how to implement an **ACL (Access Control List) server** and configure **lakeFS OSS** to
work with it. This is intended for contributors who want to understand or extend the ACL authentication mechanism in lakeFS.

Contents:

1. Required APIs for implementing an ACL server.
2. How to configure lakeFS OSS to connect to your ACL server.
3. How to run lakeFS OSS with your ACL server.

## What is ACL?

Access Control List (ACL) in lakeFS manages permissions by associating a set of permissions directly with a specific object or a group of objects. In the context of the lakeFS authorization API, ACLs are represented within policies. These policies can then be attached to users or groups to grant them the specified permissions.

## Implementation and Setup

Follow these steps to implement an ACL server compatible with lakeFS.

### 1. Implementation

To implement the ACL server, you need to implement a subset of the APIs defined in the
[authorization.yaml specification](./authorization-yaml.md).
Not all APIs in the specification are required — only those listed below, grouped into the following categories:

- **Credentials**
- **Users**
- **Groups**
- **Policies**

Implement all APIs under these categories.

{: .note}
> For detailed descriptions of the different schemas and each API, including their input and output parameters,
> refer to each API in the [authorization.yaml specification](./authorization-yaml.md).

#### Credentials APIs

These APIs are used to manage credentials (access key ID and secret access key) for users.

Implement the following endpoints under the `credentials` tag in the
[authorization.yaml specification](./authorization-yaml.md):

- `GET /auth/users/{userId}/credentials`:
  - **Description:** Returns a list of all access_key_ids and their creation dates for a specific user.
  - **Input:** `userId` (path parameter), pagination parameters (`prefix`, `after`, `amount`).
  - **Output:** A `CredentialsList` object containing a list of `Credentials` objects and pagination information.
  - **Implementation Notes:** The results should be sorted by `access_key_id`.
  - **Output Schema (`CredentialsList`):**

      ```yaml
      type: object
      properties:
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          type: array
          items:
            $ref: "#/components/schemas/Credentials"
      ```

  - **Output Schema (`Credentials`):**

      ```yaml
      type: object
      properties:
        access_key_id:
          type: string
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds
      ```

- `POST /auth/users/{userId}/credentials`:
  - **Description:** Creates new credentials for a specific user.
  - **Input:** `userId` (path parameter), optional query parameters (`access_key`, `secret_key`).
  - **Output:** A `CredentialsWithSecret` object containing the generated or provided access key ID, secret access key, creation date, and username.
  - **Implementation Notes:** If `access_key` or `secret_key` are empty, the server should generate random values. The `username` field in the response is required.
  - **Output Schema (`CredentialsWithSecret`):**

      ```yaml
      type: object
      properties:
        access_key_id:
          type: string
        secret_access_key:
          type: string
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        user_name:
          type: string
          description: A unique identifier for the user.
      ```

- `DELETE /auth/users/{userId}/credentials/{accessKeyId}`:
  - **Description:** Deletes credentials for a specific user.
  - **Input:** `userId` (path parameter), `accessKeyId` (path parameter).
  - **Output:** No output on success (HTTP 204).
  - **Implementation Notes:** Ensure the user and credentials exist before deleting.
- `GET /auth/users/{userId}/credentials/{accessKeyId}`:
  - **Description:** Returns a specific user's credentials details (excluding the secret key).
  - **Input:** `userId` (path parameter), `accessKeyId` (path parameter).
  - **Output:** A `Credentials` object containing the access key ID and creation date.
  - **Implementation Notes:** Ensure the user and credentials exist. The secret access key should not be returned by this endpoint.
  - **Output Schema (`Credentials`):**

      ```yaml
      type: object
      properties:
        access_key_id:
          type: string
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds
      ```

- `GET /auth/credentials/{accessKeyId}`:
  - **Description:** Returns the credentials details associated with a specific accessKeyId (including the secret key).
  - **Input:** `accessKeyId` (path parameter).
  - **Output:** A `CredentialsWithSecret` object containing all credential details.
  - **Implementation Notes:** This endpoint is used by lakeFS to authenticate requests using access key IDs and secret access keys. The `username` field in the response is required.
  - **Output Schema (`CredentialsWithSecret`):**

      ```yaml
      type: object
      properties:
        access_key_id:
          type: string
        secret_access_key:
          type: string
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        user_name:
          type: string
          description: A unique identifier for the user.
      ```

#### Users APIs

These APIs are used to manage users.

Implement the following endpoints under the `users` tag in the
[authorization.yaml specification](./authorization-yaml.md):

- `GET /auth/users`:
  - **Description:** Returns a list of all users.
  - **Input:** Pagination parameters (`prefix`, `after`, `amount`).
  - **Output:** A `UserList` object containing a list of `User` objects and pagination information.
  - **Implementation Notes:** The results must be sorted by the username. The `external_id` and `encryptedPassword` fields in the `User` object are not used internally by lakeFS in the ACL implementation.
  - **Output Schema (`UserList`):**

      ```yaml
      type: object
      properties:
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          type: array
          items:
            $ref: "#/components/schemas/User"
      ```

  - **Output Schema (`User`):**

      ```yaml
      type: object
      properties:
        username:
          type: string
          description: A unique identifier for the user.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        friendly_name:
          type: string
          description: A name for the user that is not necessarily unique.
        email:
          type: string
        source:
          type: string
          description: User source. Based on implementation.
      ```

- `POST /auth/users`:
  - **Description:** Creates a new user.
  - **Input:** Request body containing `UserCreation` object (`username`, optional `email`, `friendlyName`, `source`, `external_id`, `invite`).
  - **Output:** A `User` object representing the created user.
  - **Implementation Notes:** The `username` must be unique. The `external_id` and `encryptedPassword` fields in the input and output are not used internally by lakeFS in the ACL implementation. If `invite` is true, an invitation email should be sent (if supported by the implementation).
  - **Input Schema (`UserCreation`):**

      ```yaml
      type: object
      properties:
        username:
          type: string
          minLength: 1
          description: A unique identifier for the user.
        email:
          type: string
          description: If provided, the email is set to the same value as the username.
        friendlyName:
          type: string
        source:
          type: string
          description: User source. Based on implementation.
        invite:
          type: boolean
          description: A boolean that determines whether an invitation email should be sent.
      ```

  - **Output Schema (`User`):**

      ```yaml
      type: object
      properties:
        username:
          type: string
          description: A unique identifier for the user.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        friendly_name:
          type: string
          description: A name for the user that is not necessarily unique.
        email:
          type: string
        source:
          type: string
          description: User source. Based on implementation.
      ```

- `GET /auth/users/{userId}`:
  - **Description:** Returns the details of a specific user.
  - **Input:** `userId` (path parameter).
  - **Output:** A `User` object representing the user.
  - **Implementation Notes:** Ensure the user exists. The `external_id` and `encryptedPassword` fields in the `User` object are not used internally by lakeFS in the ACL implementation.
  - **Output Schema (`User`):**

      ```yaml
      type: object
      properties:
        username:
          type: string
          description: A unique identifier for the user.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        friendly_name:
          type: string
          description: A name for the user that is not necessarily unique.
        email:
          type: string
        source:
          type: string
          description: User source. Based on implementation.
      ```

- `DELETE /auth/users/{userId}`:
  - **Description:** Deletes a user.
  - **Input:** `userId` (path parameter).
  - **Output:** No output on success (HTTP 204).
  - **Implementation Notes:** Ensure the user exists. When a user is deleted, their associated credentials, group memberships, and policy attachments should also be removed.

- `GET /auth/users/{userId}/groups`:
  - **Description:** Returns the list of groups that a specific user is associated with.
  - **Input:** `userId` (path parameter), pagination parameters (`prefix`, `after`, `amount`).
  - **Output:** A `GroupList` object containing a list of `Group` objects and pagination information.
  - **Implementation Notes:** The results must be sorted by the group name.
  - **Output Schema (`GroupList`):**

      ```yaml
      type: object
      properties:
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          type: array
          items:
            $ref: "#/components/schemas/Group"
      ```

  - **Output Schema (`Group`):**

      ```yaml
      type: object
      properties:
        id:
          type: string
          description: A unique identifier of the group.
        name:
          type: string
          description: A unique identifier for the group, represented by a human-readable name.
        description:
          type: string
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
      ```

- `GET /auth/users/{userId}/policies`:
  - **Description:** Returns the list of policies associated with a specific user.
  - **Input:** `userId` (path parameter), pagination parameters (`prefix`, `after`, `amount`), optional query parameter `effective` (boolean).
  - **Output:** A `PolicyList` object containing a list of `Policy` objects and pagination information.
  - **Implementation Notes:** If `effective` is true, return all distinct policies attached to the user directly or through their groups. If `effective` is false (default), return only policies directly attached to the user.
  - **Output Schema (`PolicyList`):**

      ```yaml
      type: object
      properties:
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          type: array
          items:
            $ref: "#/components/schemas/Policy"
      ```

  - **Output Schema (`Policy`):**

      ```yaml
      type: object
      properties:
        name:
          type: string
          description: A unique, human-readable name for the policy.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        acl:
          type: string
          description: Represents the access control list assigned to this policy.
      ```

- `PUT /auth/users/{userId}/policies/{policyId}`:
  - **Description:** Attaches a policy to a specific user.
  - **Input:** `userId` (path parameter), `policyId` (path parameter).
  - **Output:** No output on success (HTTP 201).
  - **Implementation Notes:** Ensure the user and policy exist.

- `DELETE /auth/users/{userId}/policies/{policyId}`:
  - **Description:** Detaches a policy from a specific user.
  - **Input:** `userId` (path parameter), `policyId` (path parameter).
  - **Output:** No output on success (HTTP 204).
  - **Implementation Notes:** Ensure the user and policy attachment exist.

#### Groups APIs

These APIs are used to manage groups.

Implement the following endpoints under the `groups` tag:

- `GET /auth/groups`:
  - **Description:** Returns a list of groups.
  - **Input:** Pagination parameters (`prefix`, `after`, `amount`).
  - **Output:** A `GroupList` object containing a list of `Group` objects and pagination information.
  - **Implementation Notes:** The results must be sorted by the group name.
  - **Output Schema (`GroupList`):**

      ```yaml
      type: object
      properties:
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          type: array
          items:
            $ref: "#/components/schemas/Group"
      ```

  - **Output Schema (`Group`):**

      ```yaml
      type: object
      properties:
        id:
          type: string
          description: A unique identifier of the group.
        name:
          type: string
          description: A unique identifier for the group, represented by a human-readable name.
        description:
          type: string
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
      ```

- `POST /auth/groups`:
  - **Description:** Creates a new group.
  - **Input:** Request body containing `GroupCreation` object (`id`, optional `description`).
  - **Output:** A `Group` object representing the created group.
  - **Implementation Notes:** The `id` must be a unique, human-readable name for the group. This endpoint is called during setup to create initial groups.
  - **Input Schema (`GroupCreation`):**

      ```yaml
      type: object
      required:
        - id
      properties:
        id:
          type: string
          description: A unique identifier for the group, represented by a human-readable name.
        description:
          type: string
      ```

  - **Output Schema (`Group`):**

      ```yaml
      type: object
      properties:
        id:
          type: string
          description: A unique identifier of the group.
        name:
          type: string
          description: A unique identifier for the group, represented by a human-readable name.
        description:
          type: string
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
      ```

- `GET /auth/groups/{groupId}`:
  - **Description:** Returns the details of a specific group.
  - **Input:** `groupId` (path parameter).
  - **Output:** A `Group` object representing the group.
  - **Implementation Notes:** Ensure the group exists.
  - **Output Schema (`Group`):**

      ```yaml
      type: object
      properties:
        id:
          type: string
          description: A unique identifier of the group.
        name:
          type: string
          description: A unique identifier for the group, represented by a human-readable name.
        description:
          type: string
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
      ```

- `DELETE /auth/groups/{groupId}`:
  - **Description:** Deletes a group.
  - **Input:** `groupId` (path parameter).
  - **Output:** No output on success (HTTP 204).
  - **Implementation Notes:** Ensure the group exists. When a group is deleted, its associated user memberships and policy attachments should also be removed.

- `GET /auth/groups/{groupId}/members`:
  - **Description:** Returns the list of users associated with a specific group.
  - **Input:** `groupId` (path parameter), pagination parameters (`prefix`, `after`, `amount`).
  - **Output:** A `UserList` object containing a list of `User` objects and pagination information.
  - **Implementation Notes:** The results must be sorted by the username. The `external_id` and `encryptedPassword` fields in the `User` object are not used internally by lakeFS in the ACL implementation.
  - **Output Schema (`UserList`):**

      ```yaml
      type: object
      properties:
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          type: array
          items:
            $ref: "#/components/schemas/User"
      ```

  - **Output Schema (`User`):**

      ```yaml
      type: object
      properties:
        username:
          type: string
          description: A unique identifier for the user.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        friendly_name:
          type: string
          description: A name for the user that is not necessarily unique.
        email:
          type: string
        source:
          type: string
          description: User source. Based on implementation.
      ```

- `PUT /auth/groups/{groupId}/members/{userId}`:
  - **Description:** Adds a specific user to a specific group.
  - **Input:** `groupId` (path parameter), `userId` (path parameter).
  - **Output:** No output on success (HTTP 201).
  - **Implementation Notes:** Ensure the group and user exist.

- `DELETE /auth/groups/{groupId}/members/{userId}`:
  - **Description:** Removes a specific user from a specific group.
  - **Input:** `groupId` (path parameter), `userId` (path parameter).
  - **Output:** No output on success (HTTP 204).
  - **Implementation Notes:** Ensure the group and user membership exist.

- `GET /auth/groups/{groupId}/policies`:
  - **Description:** Returns the list of policies attached to a specific group.
  - **Input:** `groupId` (path parameter), pagination parameters (`prefix`, `after`, `amount`).
  - **Output:** A `PolicyList` object containing a list of `Policy` objects and pagination information.
  - **Implementation Notes:** The results must be sorted by the policy name.
  - **Output Schema (`PolicyList`):**

      ```yaml
      type: object
      properties:
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          type: array
          items:
            $ref: "#/components/schemas/Policy"
      ```

  - **Output Schema (`Policy`):**

      ```yaml
      type: object
      properties:
        name:
          type: string
          description: A unique, human-readable name for the policy.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        acl:
          type: string
          description: Represents the access control list assigned to this policy.
      ```

- `PUT /auth/groups/{groupId}/policies/{policyId}`:
  - **Description:** Attaches a policy to a specific group.
  - **Input:** `groupId` (path parameter), `policyId` (path parameter).
  - **Output:** No output on success (HTTP 201).
  - **Implementation Notes:** Ensure the group and policy exist.

- `DELETE /auth/groups/{groupId}/policies/{policyId}`:
  - **Description:** Detaches a policy from a specific group.
  - **Input:** `groupId` (path parameter), `policyId` (path parameter).
  - **Output:** No output on success (HTTP 204).
  - **Implementation Notes:** Ensure the group and policy attachment exist.

#### Policies APIs

These APIs are used to manage policies, which contain the ACL information.

Implement the following endpoints under the `policies` tag:

- `GET /auth/policies`:
  - **Description:** Returns a list of policies.
  - **Input:** Pagination parameters (`prefix`, `after`, `amount`).
  - **Output:** A `PolicyList` object containing a list of `Policy` objects and pagination information.
  - **Implementation Notes:** The results must be sorted by the policy name.
  - **Output Schema (`PolicyList`):**

      ```yaml
      type: object
      properties:
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          type: array
          items:
            $ref: "#/components/schemas/Policy"
      ```

  - **Output Schema (`Policy` - relevant fields for ACL):**

      ```yaml
      type: object
      properties:
        name:
          type: string
          description: A unique, human-readable name for the policy.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        acl:
          type: string
          description: Represents the access control list assigned to this policy.
      ```

- `POST /auth/policies`:
  - **Description:** Creates a new policy.
  - **Input:** Request body containing a `Policy` object (`name`, `statement` and `acl`).
  - **Output:** A `Policy` object representing the created policy.
  - **Implementation Notes:** The `name` must be a unique, human-readable name for the policy. The `acl` property in the `Policy` object is used to define the permissions. The `statement` property is not used in the ACL implementation. This endpoint is called during setup to create default policies.
  - **Input Schema (`Policy` - relevant fields for ACL):**

      ```yaml
      type: object
      properties:
        name:
          type: string
          description: A unique, human-readable name for the policy.
        acl:
          type: string
          description: Represents the access control list assigned to this policy.
      ```

  - **Output Schema (`Policy`):**

      ```yaml
      type: object
      properties:
        name:
          type: string
          description: A unique, human-readable name for the policy.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        acl:
          type: string
          description: Represents the access control list assigned to this policy.
      ```

- `GET /auth/policies/{policyId}`:
  - **Description:** Returns the details of a specific policy.
  - **Input:** `policyId` (path parameter).
  - **Output:** A `Policy` object representing the policy.
  - **Implementation Notes:** Ensure the policy exists. The `statement` property is not used in the ACL implementation.
  - **Output Schema (`Policy`):**

      ```yaml
      type: object
      properties:
        name:
          type: string
          description: A unique, human-readable name for the policy.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        acl:
          type: string
          description: Represents the access control list assigned to this policy.
      ```

- `PUT /auth/policies/{policyId}`:
  - **Description:** Updates an existing policy.
  - **Input:** `policyId` (path parameter), request body containing the updated `Policy` object.
  - **Output:** A `Policy` object representing the updated policy.
  - **Implementation Notes:** Ensure the policy exists and the provided `policyId` matches the `name` in the request body. The requestis to update the `acl` property. The `statement` property is not used in the ACL implementation.
  - **Input Schema (`Policy`):**

      ```yaml
      type: object
      properties:
        name:
          type: string
          description: A unique, human-readable name for the policy.
        acl:
          type: string
          description: Represents the access control list assigned to this policy.
      ```

  - **Output Schema (`Policy`):**

      ```yaml
      type: object
      properties:
        name:
          type: string
          description: A unique, human-readable name for the policy.
        creation_date:
          type: integer
          format: int64
          description: Unix Epoch in seconds.
        acl:
          type: string
          description: Represents the access control list assigned to this policy.
      ```

- `DELETE /auth/policies/{policyId}`:
  - **Description:** Deletes a policy.
  - **Input:** `policyId` (path parameter).
  - **Output:** No output on success (HTTP 204).
  - **Implementation Notes:** Ensure the policy exists. When a policy is deleted, its attachments to users and groups should also be removed.

### 2. Setup

#### Key Steps in the Initial Setup

When deploying an ACL server for the first time, it is essential to establish a set of standard user groups and assign each group a default set of permissions (policies). This process ensures that the system starts with a clear structure for access control, making it easy to manage users and their roles securely and consistently.

**Define Standard Groups**

**Define Standard Groups**
Establish a set of base groups that represent the typical roles in your system, such as Admins (full privileges), Writers (read/write access), and Readers (read-only access). Each group should be mapped to a specific policy that defines the permissions for its members.

You can reference the [lakeFS contrib ACL implementation](https://github.com/treeverse/lakeFS/blob/master/contrib/auth/acl/setup.go) to see practical examples of how standard groups are defined and structured, along with their associated permission policies.
#### lakeFS Configuration

Update your lakeFS configuration file (`config.yaml`) to include:

```yaml
auth:
  encrypt:
    secret_key: "some_string"
  ui_config:
    rbac: "simplified"
  api:
    endpoint: {ENDPOINT_TO_YOUR_ACL_SERVER} # e.g., http://localhost:9006/api/v1
    token: {ACL_SERVER_TOKEN} # Used as authentication bearer calling the ACL server
```

{: .note}
> The `auth.api.token` parameter is optional. If unspecified, lakeFS uses the `auth.encrypt.secret_key` as
> the secret to generate JWT. If specified, provide a JWT token or via the environment variable `LAKEFS_AUTH_API_TOKEN`.
