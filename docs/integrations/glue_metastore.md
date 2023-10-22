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

### What is supported 

- Creating a uniqueue table in Glue Catalog per lakeFS repository / ref / commit. 
- No data copying is required, the table location is a path to a symlinks structure in S3 based on Hive's [SymlinkTextInputFormat](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/io/SymlinkTextInputFormat.html) and the [table partitions](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html#tables-partition) are maintained.
- Tables are described via [Hive format in _lakefs_tables/<my_table>.yaml](({% link integrations/catalog_exports.md %}#hive-tables))
- Currently the data query in Glue metastore is Read-Only operation and mutating data requires writting to lakeFS and letting the export hook run.

### Pre-requisite 

1. Write some lakeFS table data (i.e [Spark]({% link integrations/spark.md %}) or some CSV)
2. AWS Credentials with permission to manage Glue, Athena Query and S3 access.
3. lakeFS [Actions]({% link howto/hooks/index.md %}) enabled. 