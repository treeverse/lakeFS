---
layout: default
title: Database Migration
description: A guide to migrating lakeFS database.
parent: Reference
nav_order: 51
has_children: false
---

# lakeFS Database Migrate
{: .no_toc }

**Note:** Feature in development
{: .note }

The lakeFS database migration tool simplifies switching from one database implementation to another.
More information can be found [here](https://github.com/treeverse/lakeFS/issues/3899)

## lakeFS with Key Value Store

Starting at version 0.80.0, lakeFS abandoned the tight coupling to [PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) and moved all database operations to work over [Key-Value Store](https://en.wikipedia.org/wiki/Key%E2%80%93value_database)
To better understand how lakeFS uses KV Store, please refer to [KV in a Nutshell](../understand/kv-in-a-nutshell.html)

