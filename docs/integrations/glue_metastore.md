---
title: Glue / Athena support
description: This section explains how to query data from lakeFS branches in services backed by Glue Metastore in particular Athena.
parent: Integrations
redirect_from: /using/glue_metastore.html
---


# Using lakeFS with the Glue Metastore


{% include toc_2-3.html %}

## About Glue Metastore 

This part explains about how Glue Metastore work with lakeFS.

[AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html) has a metastore that can store metadata related to Hive and other services (such as Spark and Trino). It has metadata such as the location of the table, information about columns, partitions and many more.


## Support in lakeFS

The integration between Glue and lakeFS is based on [Data Catalog Exports](({% link integrations/catalog_exports.md %})).
It allows defining a table 


