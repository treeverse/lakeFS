---
title: Delta Lake
description: This section explains how to use Delta Lake with lakeFS.
parent: Integrations
---

# Using lakeFS with Delta Lake

[Delta Lake](https://delta.io/) Delta Lake is an open-source storage framework designed to improve performance and provide transactional guarantees to data lake tables.

Because lakeFS is format-agnostic, you can save data in Delta format within a lakeFS repository and benefit from the advantages of both technologies.  Specifically:

1. ACID operations can span across many Delta tables.
2. [CI/CD hooks][data-quality-gates] can validate Delta table contents, schema, or even referential integrity.
3. lakeFS supports zero-copy branching for quick experimentation with full isolation.

{% include toc.html %}

## Using Delta Lake with lakeFS from Apache Spark

_Given the native integration between Delta Lake and Spark, it's most common that you'll interact with Delta tables in a Spark environment._

To configure a Spark environment to read from and write to a Delta table within a lakeFS repository, you need to set the proper credentials and endpoint in the S3 Hadoop configuration, like you'd do with any [Spark](./spark.md) environment.

Once set, you can interact with Delta tables using regular Spark path URIs. Make sure that you include the lakeFS repository and branch name:

```scala
df.write.format("delta").save("s3a://<repo-name>/<branch-name>/path/to/delta-table")
```

Note: If using the Databricks Analytics Platform, see the [integration guide](./spark.md#installation) for configuring a Databricks cluster to use lakeFS.

To see the integration in action see [this notebook](https://github.com/treeverse/lakeFS-samples/blob/main/00_notebooks/delta-lake.ipynb) in the [lakeFS Samples Repository](https://github.com/treeverse/lakeFS-samples/).

## Using Delta Lake with lakeFS from Python

The [delta-rs](https://github.com/delta-io/delta-rs) library provides bindings for Python. This means that you can use Delta Lake and lakeFS directly from Python without needing Spark. Integration is done through the [lakeFS S3 Gateway]({% link understand/architecture.md %}#s3-gateway)

The documentation for the `deltalake` Python module details how to [read](https://delta-io.github.io/delta-rs/python/usage.html#loading-a-delta-table), [write](https://delta-io.github.io/delta-rs/python/usage.html#writing-delta-tables), and [query](https://delta-io.github.io/delta-rs/python/usage.html#querying-delta-tables) Delta Lake tables. To use it with lakeFS use an `s3a` path for the table based on your repository and branch (for example, `s3a://delta-lake-demo/main/my_table/`) and specify the following `storage_options`:

```python
storage_options = {"AWS_ENDPOINT": <your lakeFS endpoint>,
                   "AWS_ACCESS_KEY_ID": <your lakeFS access key>,
                   "AWS_SECRET_ACCESS_KEY": <your lakeFS secret key>,
                   "AWS_REGION": "us-east-1",
                   "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
                  }
```

If your lakeFS is not using HTTPS (for example, you're just running it locally) then add the option

```python
                   "AWS_STORAGE_ALLOW_HTTP": "true"
```

To see the integration in action see [this notebook](https://github.com/treeverse/lakeFS-samples/blob/main/00_notebooks/delta-lake-python.ipynb) in the [lakeFS Samples Repository](https://github.com/treeverse/lakeFS-samples/).


## Best Practices

Production workflows should ideally write to a single lakeFS branch that could then be safely merged into `main`. This is because the [Delta log](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html) is an auto-generated sequence of text files used to keep track of transactions on a Delta table sequentially. Writing to one Delta table from multiple lakeFS branches is possible, but note that it would result in conflicts if later attempting to merge one branch into the other.

### When running lakeFS inside your VPC (on AWS)

When lakeFS runs inside your private network, your Databricks cluster needs to be able to access it. 
This can be done by setting up a VPC peering between the two VPCs 
(the one where lakeFS runs and the one where Databricks runs). For this to work on Delta Lake tables, you would also have to disable multi-cluster writes with:

```
spark.databricks.delta.multiClusterWrites.enabled false
```

### Using multi cluster writes (on AWS)

When using multi-cluster writes, Databricks overrides Delta’s S3-commit action. 
The new action tries to contact lakeFS from servers on Databricks’ own AWS account, which of course won’t be able to access your private network. 
So, if you must use multi-cluster writes, you’ll have to allow access from Databricks’ AWS account to lakeFS. 
If you are trying to achieve that, please reach out on Slack and the community will try to assist.

## Further Reading

See [Guaranteeing Consistency in Your Delta Lake Tables With lakeFS](https://lakefs.io/blog/guarantee-consistency-in-your-delta-lake-tables-with-lakefs/) post on the lakeFS blog to learn how to 
guarantee data quality in a Delta table by utilizing lakeFS branches.


[data-quality-gates]:  {% link understand/use_cases/cicd_for_data.md %}#using-hooks-as-data-quality-gates
[deploy-docker]:  {% link howto/deploy/onprem.md %}#docker
