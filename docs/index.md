---
layout: default
title: Home
nav_order: 0
---

# lakeFS Documentation



### lakeFS is a Data Lake Management platform that enables ACID guarantees using Git-like operations on conventional Object Stores (i.e. S3)  
{: .fw-300 }

## Key Features

* Git-like Branching & Committing for large scale data, providing a simple way to isolate and manage changes
* Instant and atomic rollback of changes, making mistakes cheap
* Strongly consistent data operations
* Transactional nature creates a clear contract between writers and readers (i.e. if a partition exists, it's *guaranteed* to contain all its data)
* Object level deduplication - multiple copies of objects are physically stored only once
* API Compatibility with S3 means it's a drop-in replacement, with the actual data itself being stored on S3


[Read More](what_is_lakefs.html){: .btn .btn-outline }  [Quick Start](quickstart.md){: .btn .btn-blue }
{: .cta }
