---
layout: default
title: Hard Deletion
description: Data retention feature to clean up unnecessary objects
parent: Reference
nav_order: 25
has_children: false
---

# Garbage Collection
{: .no_toc }

By default, lakeFS keeps all your objects forever. This allows you to travel back in time to previous versions of your data.
However, sometimes you may want to hard-delete your objects, namely delete them from the underlying storage. 
Reasons for this include cost-reduction and privacy laws.

Garbage collection rules in lakeFS define how long to retain objects after they are deleted.
After running a GC job, objects that have been deleted prior to the retention period are hard-deleted.
The GC job does not remove any commits: you will still be able to use commits containing hard-deleted objects,
but trying to read these objects from lakeFS will result in a `409 Gone` HTTP status.

## Understanding Garbage Collection

For every branch, the GC job retains deleted objects for the number of days defined for the branch.
In the absence of a branch-specific rule, the default rule for the repository is used.
If an object is present in more than one branch ancestry, it is retained according to the rule with the largest number of days between those branches.
That is, it is hard-deleted only after the retention period has ended for all relevant branches.

**Note:** In order for an object to be hard-deleted, it must be deleted from all branches.
You should delete stale branches to prevent them from retaining old objects.
{: .note }

Example GC rules for a repository:
```json
{
  "default_retention_days": 21,
  "branches": [
    {"branch_id":  "main", "retention_days":  28},
    {"branch_id":  "dev", "retention_days":  7}
  ]
}
```

In the above example, objects are retained for 21 days after deletion by default. However, if they are present in the main `branch`, they are retained for 28 days.
Objects present in the `dev` branch (but not in the `main` branch), are retained for 7 days after they are deleted.

## Configuring GC rules

Use the `lakectl` CLI to define the GC rules: 

```bash
cat <<EOT >> example_repo_gc_rules.json
{
  "default_retention_days": 21,
  "branches": [
    {"branch_id":  "main", "retention_days":  28},
    {"branch_id":  "dev", "retention_days":  7}
  ]
}
EOT

lakectl gc set-config lakefs://example-repo -f example_repo_gc_rules.json 
```

## Running the GC job

