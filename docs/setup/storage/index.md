---
layout: default
title: Prepare Your Storage
description: This section explains how to configure the underlying storage layer.
parent: Setup lakeFS
nav_order: 1
has_children: true
redirect_from:
- ../deploying-aws/setup.html
- ../deploying-aws/bucket.html
---
  
# Prepare Your Storage

A production installation of lakeFS will usually use your cloud provider's object storage as the underlying storage layer.
You can create a new bucket/container (recommended) or use an existing one with a path prefix.
The path under the existing bucket/container should be empty.

If you already have a bucket/container configured, you're ready to [create your first lakeFS repository](../create-repo.md).

Choose your storage provider to configure your storage:
