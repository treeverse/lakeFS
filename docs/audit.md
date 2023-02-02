---
layout: default
title: Audit
description: lakeFS Cloud audit log
nav_order: 100
has_children: false
---


Audit logs is a premium feature provided only in [lakeFS cloud](https://lakefs.cloud/).
{: .note .note-info }

# Audit log

The lakeFS audit log allows you to view all relevant user action information in a clear and organized table,

![audit log](assets/img/audit-log.png)

## Audit Fields

The audit log includes the following fields 
- Time - Time of action
- User - Name of the user who performed the action
- Region - Region of the lakeFS installation where the action was performed
- Status - Status code returned for the action
  - 2xx - Successful
  - 3xx - Redirection
  - 4xx - Client error
  - 5xx - Server error
- Action - Specific lakeFS action (such as Login, Commit, ListRepositories, CreateUser, etc...)
- Resource - Full URL of the command (e.g for a commit on branch `main` we would see the Action `commit` and the resource `/api/v1/repositories/e2e-monitoring/branches/main/commits`)

## Filtering

Filtering is available using the filter bar. the filter bar works with a simple query language.
The table fields could be filter by the following operators
- User  - `=`,`!=`
- Region - `=`,`!=`
- Status(Number) - `<`,`>`,`<=`,`>=`,`=`,`!=`
- Action - `=`,`!=`
- Resource - `=`,`!=`
