---
layout: default
title: Using R with lakeFS
description: How to use lakeFS from R including creating branches, committing changes, and merging.
parent: Integrations
nav_order: 60
has_children: false
---

# Using R with lakeFS

R is a powerful language used widely in data science. 

lakeFS interfaces with R in two ways: 

* the [S3 gateway](https://docs.lakefs.io/understand/architecture.html#s3-gateway) which presents a lakeFS repository as an S3 bucket. You can then read and write data in lakeFS using standard S3 tools such as the `aws.s3` library.
* a [rich API](https://docs.lakefs.io/reference/api.html) for which can be accessed from R using the `httr` library. Use the API for working with branches and commits.

{% include toc.html %}

## Reading and Writing from lakeFS with R

Working with data stored in lakeFS from R is the same as you would with an S3 bucket, via the [S3 Gateway that lakeFS provides](https://docs.lakefs.io/understand/architecture.html#s3-gateway).

You can use any library that interfaces with S3. In this example we'll use the [aws.s3](https://github.com/cloudyr/aws.s3/tree/master) library.

```r
install.packages(c("aws.s3"))
```

