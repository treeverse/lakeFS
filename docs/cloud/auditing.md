---
layout: default
title: Auditing
parent: lakeFS Cloud
description: Auditing is a solution for lakeFS Cloud which enables tracking of events and activities performed within the solution. These logs capture information such as who accessed the solution, what actions were taken, and when they occurred.
has_children: false
redirect_from: 
  - /audit.html
  - /reference/audit.html
---

# Auditing
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

{: .note}
> Auditing is only available for [lakeFS Cloud](../cloud/).

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
- Action - Specific lakeFS action (such as _Login_, _Commit_, _ListRepositories_, _CreateUser_, etc...)
- Resource - Full URL of the command (e.g for a commit on branch `main` we would see the action _Commit_ and the resource `/api/v1/repositories/e2e-monitoring/branches/main/commits`)

## Filtering

Filtering is available using the filter bar. The filter bar works with a simple query language.

The table fields can be filtered by the following operators
- User  - `=`,`!=`
- Region - `=`,`!=`
- Status(Number) - `<`,`>`,`<=`,`>=`,`=`,`!=`
- Action - `=`,`!=`
- Resource - `=`,`!=`