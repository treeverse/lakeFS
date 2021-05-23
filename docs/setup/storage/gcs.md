---
layout: default
title: Google Cloud Storage
description: This guide explains how to configure Google Cloud Storage as the underlying storage layer.
parent: Prepare Your Storage
grand_parent: Setup lakeFS
nav_order: 20
has_children: false
---

# Prepare Your GCS Bucket

1. On the Google Cloud Storage console, click *Create Bucket*. Follow the instructions.

1. On the *Permissions* tab, add the service account you intend to use lakeFS with. Give it a role that allows reading and writing to the bucket, e.g. *Storage Object Creator*.

You are now ready to [create your first lakeFS repository](../create-repo.md).
