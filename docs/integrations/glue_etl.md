---
layout: default
title: Glue ETL 
description: This section covers how you can start using lakeFS with AWS Glue ETL.
parent: Integrations
nav_order: 66
has_children: false
redirect_from: ../using/glue_etl.html
---

# Using lakeFS with Glue ETL
[AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html){: .button-clickable} is a fully managed extract, transform, and load (ETL) service. With AWS Glue ETL you can run your ETL jobs as soon as new data becomes available in Amazon S3 by invoking your AWS Glue ETL jobs from an AWS Lambda function.

## Configuration
Since Glue ETL is essentially running Spark jobs, to configure Glue ETL to work with lakeFS, you should apply the [lakeFS Spark configuration](spark.md#configuration){: .button-clickable} to your Glue ETL script.   
