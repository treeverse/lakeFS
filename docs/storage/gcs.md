---
layout: default
title: Google Cloud Storage
description:
parent: Prepare Your Storage
nav_order: 30
has_children: false
---

# Prepare Your GCS Bucket

1. On the Google Cloud Storage console, click *Create Bucket*. Follow the instructions.

1. On the *Permissions* tab, add the service account you intend to use lakeFS with. Give it a role that allows reading and writing to the bucket, e.g. *Storage Object Creator*.

You can now proceed to [Installing lakeFS](../deploy/gcp.md).
