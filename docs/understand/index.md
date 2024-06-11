---
title: Understanding lakeFS
description: Details about lakeFS Concepts and Design
nav_order: 15
has_children: true
has_toc: false
---

# Understanding lakeFS

<img src="/assets/img/docs_logo.png" alt="lakeFS Docs" width=200 style="float: right; margin: 0 0 10px 10px;"/>

## Architecture and Internals

The [Architecture](./architecture.html) page includes a logical overview of lakeFS and its components. 

For deep-dive content about lakeFS see: 

* [Internal database structure](./how/kv.md)
* [Merges in lakeFS](./how/merge.md)
* [Versioning Internals](./how/versioning-internals.md)

## lakeFS Use Cases

lakeFS has many uses in the data world, including

* [CI/CD for Data Lakes](./use_cases/cicd_for_data.md)
* [ETL Testing Environment](./use_cases/etl_testing.md)
* [Reproducibility](./use_cases/reproducibility.md)
* [Rollback](./use_cases/rollback.md)

One of the important things that lakeFS provides is full support for [Data Lifecycle Management](./data_lifecycle_management/) through all stages: 

* [In Test](./data_lifecycle_management/data-devenv.md)
* [During Deployment](./data_lifecycle_management/ci.md)
* [In Production](./data_lifecycle_management/production.md)

## lakeFS Concepts and Model 

lakeFS adopts many of the terms and concepts from git. [This page](./model.html) goes into details on the similarities and differences, and provides a good background to the concepts used in lakeFS. 

## Performance

Check out the [Performance best practices](./performance-best-practices.html) guide for useful hints and tips on ensuring high performance from lakeFS. 

## FAQ and Glossary

The [FAQ](./faq.html) covers many common questions around lakeFS, and the [glossary](./glossary.html) provides a useful reference for the terms used in lakeFS.
