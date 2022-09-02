---
layout: default
title: Advanced features of lakeFS
description: Learn more about the advanced features of lakeFS.
nav_order: 10
has_children: true
---

# Advanced features of lakeFS

This section explores all the key concepts and mechanisms underlying lakeFS. Take a look at these sections to learn how lakeFs works under the hood.

## Object model

lakeFS brings together concepts from object stores such as S3 with concepts from Git. 
This reference defines the common concepts of lakeFS.

[Learn more](./model.md)

## Protected branches

lakeFS users can define branch protection rules to prevent direct changes and commits to specific branches. Only merges are allowed into protected branches. 
Together with pre-merge hooks, users can run validations on their data before it reaches important branches and is exposed to consumers.

[Learn more](./protected_branches.md)

## Exporting data

This features comes in handy when external data consumers don’t have access to your lakeFS installation, some data pipelines aren't fully migrated with lakeFS,
or you'd like to experiment with lakeFS as a side-by-side installation first. You can use the export operation to copy all data from a given lakeFS commit to a designated object store location.

[Learn more](./export.md)

## Garbage collection

By default, lakeFS keeps all your objects forever, allowing you to travel back in time to the previous versions of your data. 
But sometimes you may want to hard-delete your objects from the underlying storage for cost reduction or compliance. This is possible in lakeFS. 

[Learn more](./garbage-collection.md)

