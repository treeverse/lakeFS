---
title: Access Control Lists (ACLs) -Deprecated-
description: Access control lists (ACLs) are one of the resource-based options that you can use to manage access to your repositories and objects. There are limits to managing permissions using ACLs.
parent: Security
redirect_from:
  - /reference/access-control-list.html
  - /reference/access-control-lists.html
---

# RBAC server implementation

## Table of Content
TODO - add

## Overview

This document explains how to implement an RBAC server and configure it with lakeFS to enable RBAC capabilities.
You can find an explanation of the RBAC model in lakeFS [here](./rbac.html). 

## What is RBAC?

RBAC (Role-Based Access Control) in lakeFS is a permission management model that assigns access rights based on roles 
rather than individual users. It simplifies access control by grouping permissions into predefined roles, 
which consist of specific actions. Users are assigned roles based on their responsibilities, and access is granted 
accordingly. This approach manages permissions at the role level instead of configuring access per user.


//TODO - from here rephrase with GPT:

## Setting up the server

In order to implement RBAC so that it will work with lakeFS follow theses steps:
1. ### Implementation
   The header of each response must include the 'Content-Type' key and it's value should include the 'json' keyword.
   You should implement some of the APIs from the authentication.yaml spec(add the link!!!!!!!!!!!!!!!!). In theses categories:
   
   Implement the endpoints that are listed here. All the rest APIs in the spec are internal so you don't need to implement them.

1. Credentials:
   The endpoints descriptions, inputs and outputs are described in the authentication.yaml spec(add the link!!!!!!!!!!!!!!!!) 
   under the tag "credentials" in the "tags" in the spec.
   Implement these APIs:
   1. GET /auth/users/{userId}/credentials - Anna - done
   2. POST /auth/users/{userId}/credentials - Anna - done
   3. DELETE /auth/users/{userId}/credentials/{accessKeyId} - Anna - done
   4. GET /auth/users/{userId}/credentials/{accessKeyId} - Anna - done
   5. GET /auth/credentials/{accessKeyId} - Anna - didn't do!!!!!!

2. Users: 
   The endpoints descriptions, inputs and outputs are described in the authentication.yaml spec(add the link!!!!!!!!!!!!!!!!)
   under the tag "users" in the "tags" in the spec.
   Implement these APIs:
   1. GET /auth/users - Anna - done
   2. POST /auth/users - Anna - done
   3. GET /auth/users/{userId} - Anna - done
   4. DELETE /auth/users/{userId} - Anna - done
   5. PUT /auth/users/{userId}/password - Anna - didn't do because service and controller dont use this func.
   6. PUT /auth/users/{userId}/friendly_name - Anna - didn't do because controller dont use this func.
   7. GET /auth/users/{userId}/groups - Anna - done
   8. GET /auth/users/{userId}/policies
   9. PUT /auth/users/{userId}/policies/{policyId}
   10. DELETE /auth/users/{userId}/policies/{policyId} - Anna - done

3. Groups: 
   The endpoints descriptions, inputs and outputs are described in the authentication.yaml spec(add the link!!!!!!!!!!!!!!!!)
   under the tag "groups" in the "tags" in the spec.
   Implement these APIs:
   1. GET /auth/groups - Anna - done
   2. POST /auth/groups - Anna - done
   3. GET /auth/groups/{groupId} - Anna - done
   4. DELETE /auth/groups/{groupId} - Anna - done
   5. GET /auth/groups/{groupId}/members - Anna - done
   6. PUT /auth/groups/{groupId}/members/{userId} - Anna - done
   7. DELETE /auth/groups/{groupId}/members/{userId} - Anna - done
   8. GET /auth/groups/{groupId}/policies - Anna - done
   9. PUT /auth/groups/{groupId}/policies/{policyId} - Anna - done
   10. DELETE /auth/groups/{groupId}/policies/{policyId} - Anna - done

4. Policies:
   The endpoints descriptions, inputs and outputs are described in the authentication.yaml spec(add the link!!!!!!!!!!!!!!!!)
   under the tag "policies" in the "tags" in the spec.
   Implement these APIs:
   1. GET /auth/policies - Anna - done
   2. POST /auth/policies - Anna - done
   3. GET /auth/policies/{policyId} - Anna - done
   4. PUT /auth/policies/{policyId} - Anna - done
   5. DELETE /auth/policies/{policyId} - Anna - done

2. ### Configuration
   1. Configure the lakeFS config:
      Add to your lakeFS config the following fields:
      ```
      auth:
        encrypt:
          secret_key: "some_string"
        ui_config:
          rbac: internal
        api:
          endpoint: {ENDPOINT_TO_YOUR_RBAC_SERVER} for example: http://localhost:9006/api/v1
          token:
      ```
      {: .note }
      > the auth.api.token parameter isn't required. If you don't specify it, lakeFS will use the 
      > auth.encrypt.secret_key parameter as the token.
      > If you do specify it, you can pass your JWT token as specified here or you can pass it as a environment
      > variable: LAKEFS_AUTH_API_TOKEN
2. ### Setup
   In the first time lakeFS is geting up, it creates the first users and groups and policies. In lakeFS, once he 
   completes the initialization to users, groups and policies, i.e the authorization method was set, it can't be
   changed. It means that once lakeFS runs without the auth (RBAC) server and then tries to connect to this auth
   (RBAC) server, he won't be able to enter to lakeFS. It means that you will have to start lakeFS from scratch.
3. ### Running
   1. Run your server 
   2. Run lakeFS using the new config file
      ```
      .{LAKEFS_DIR}/lakefs -c {PATH_TO_YOUR_CONFIG_FILE} run
      ```