---
title: Unity Catalog
description: Accessing lakeFS-exported Delta Lake tables from Unity Catalog.
parent: Integrations
redirect_from: /using/unity_catalog.html
---

# Using lakeFS with the Unity Catalog

{% include toc_2-3.html %}

## Overview

Databricks Unity Catalog is a centralized data governance platform for managing your data lakes.
Using Unity Catalog you can easily search for and find data assets across all workspaces using a unified catalog. 
Using Unity Catalog's external tables feature, it's possible to register a lakeFS-exported Delta Lake table and
access it from the unified catalog.
The following step-by-step guide will walk you through the step of configuring a [Lua hook]({% link howto/hooks/lua.md %})
that exports Delta Lake tables from lakeFS, and then registers them in Unity Catalog.

## Prerequisites

Before starting, make sure you have:

1. Access to Unity Catalog
2. An active lakeFS installation with S3 as the backing storage, and a repository in this installation.
3. A Databricks SQL warehouse.
4. AWS Credentials with S3 access.
5. lakeFS credentials with access to your Delta Tables.

### Databricks authentication

Since the hook will eventually register a table in Unity Catalog, authentication is required with Databricks:

1. The hook will authenticate with Databricks using a [Service Principal](https://docs.databricks.com/en/dev-tools/service-principals.html)
and an associated [token](https://docs.databricks.com/en/dev-tools/service-principals.html#step-4-generate-a-databricks-personal-access-token-for-the-databricks-service-principal).
2. The service principal should have `Service principal: Manager` over itself (Workspace: Admin console -> Service principals -> `<service principal>` -> Permissions -> Grant access (`<service principal>`:
   `Service principal: Manager`), `Workspace access` and `Databricks SQL access` checked (Admin console -> Service principals -> `<service principal>` -> Configurations).
3. Allow the service principal to use your SQL warehouse (SQL Warehouses -> `<SQL warehouse>` -> Permissions -> `<service principal>`: `Can use`).
4. 

### Add table descriptor

Let's define a table, and commit it to lakeFS.
Save the YAML below as `animals.yaml` and upload it to lakeFS. 
