---
layout: default
title: What is lakeFS
description: A lakeFS documentation website that provides information on how to use lakeFS to deliver resilience and manageability to data lakes.
nav_order: 0
redirect_from: ./downloads.html
---

{: .no_toc }

lakeFS transforms object storage buckets into data lake repositories that expose a Git-like interface. By design, it works with data of any size.

## Same workflows for code and data

The Git-like interface means users of lakeFS can use the same development workflows for code and data. Git workflows greatly improved software development practices; we designed lakeFS to bring the same benefits to data.

In this way, lakeFS brings a unique combination of performance and manageability to data lakes. To learn more about applying Git principles to data, [see here](https://lakefs.io/how-to-manage-your-data-the-way-you-manage-your-code/).

## Supports the major cloud providers

The open source lakeFS project supports AWS S3, Azure Blob Storage and Google Cloud Storage (GCS) as its underlying storage service. It is API compatible with S3 and integrates seamlessly with popular data frameworks such as Spark, Hive, dbt, Trino, and many others.

## NEW lakeFS Playground

Experience lakeFS first hand with your own isolated environment. You can easily integrate it with your existing tools, and feel lakeFS in action in an environment similar to your own.

> [Try lakeFS now without installing](https://demo.lakefs.io/)

## How do I use lakeFS?

lakeFS maintains compatibility with the S3 API to minimize adoption friction. Use it as a drop-in replacement for S3 from the perspective of any tool interacting with a data lake.

For example, take the common operation of reading a collection of data from object storage into a Spark DataFrame. For data outside a lakeFS repo, the code will look like:

> df = spark.read.parquet("s3a://my-bucket/collections/foo/")
After adding the data collections in my-bucket to a repository, the same operation becomes:

> df = spark.read.parquet("s3a://my-repo/main-branch/collections/foo/")

You can use the same methods and syntax you already use to read and write data when using a lakeFS repository. This simplifies adoption of lakeFS: minimal changes are needed to get started, making further changes an incremental process.

