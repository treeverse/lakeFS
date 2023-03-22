---
layout: default
title: Audit Logs
parent: Reference
description: lakeFS Audit Logs
nav_order: 100
has_children: false
redirect_from: "/audit.html"
---

The Audit logs feature is only available in [lakeFS Cloud](https://lakefs.cloud/).
{: .note .note-info }

# Audit log

The lakeFS audit log allows you to view all relevant user action information in a clear and organized table, including when the action was performed, by whom, and what it was they did. 

![audit log](/assets/img/audit-log.png)

This can be useful for several purposes, including: 

1. Compliance - Audit logs can be used to show what data users accessed, as well as any changes they made to user management.

2. Troubleshooting - If something changes on your underlying object store that you weren't expecting, such as a big file suddenly breaking into thousands of smaller files, you can use the audit log to find out what action led to this change. 

## Audit Fields

The audit log includes the following fields:

- Time - Time of action
- User - Name of the user who performed the action
- Region - Region of the lakeFS installation where the action was performed
- Status - Status code returned for the action
  - 2xx - Successful
  - 3xx - Redirection
  - 4xx - Client error
  - 5xx - Server error
- Action - Specific lakeFS action (such as `Login`, `Commit`, `ListRepositories`, `CreateUser`, etc...)
- Resource - Full URL of the command (e.g for a commit on branch `main` we would see the Action `Commit` and the resource `/api/v1/repositories/e2e-monitoring/branches/main/commits`)

## Filtering

Filtering is available using the filter bar. The filter bar works with a simple query language.

The table fields can be filtered by the following operators
- User  - `=`,`!=`
- Region - `=`,`!=`
- Status(Number) - `<`,`>`,`<=`,`>=`,`=`,`!=`
- Action - `=`,`!=`
- Resource - `=`,`!=`