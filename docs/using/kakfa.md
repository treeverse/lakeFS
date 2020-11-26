---
layout: default
title: Kafka
description: This section covers how you can start using lakeFS with Kafka using Confluent’s S3 Sink Connector
parent: Using lakeFS with...
nav_order: 12
has_children: false
---

# Using lakeFS with Kafka

{: .no_toc }
[Apache Kafka](https://kafka.apache.org/) provides a unified, high-throughput, low-latency platform for handling real-time data feeds.

Different distributions of Kafka have different methods for exporting data to s3, called Kafka Sink Connectors.

Most commonly used for S3 is [Confluent's S3 Sink Connector](https://docs.confluent.io/current/connect/kafka-connect-s3/index.html).

Add the following to `connector.properties` file for lakeFS support:

```properties
# Your lakeFS repository
s3.bucket.name=example-repo

# Your lakeFS S3 endpoint and credentials
store.url=https://s3.lakefs.example.com
aws.access.key.id=AKIAIOSFODNN7EXAMPLE
aws.secret.access.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# master being the branch we want to write to
topics.dir=master/topics 
```
