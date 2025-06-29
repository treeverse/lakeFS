---
title: Apache Kafka
description: This section explains how you can start using lakeFS with Kafka using Confluent’s S3 Sink Connector.
---

# Using lakeFS with Apache Kafka

[Apache Kafka](https://kafka.apache.org/) provides a unified, high-throughput, low-latency platform for handling real-time data feeds.

Different distributions of Kafka offer different methods for exporting data to S3 called Kafka Sink Connectors.

The most commonly used Connector for S3 is [Confluent's S3 Sink Connector](https://docs.confluent.io/current/connect/kafka-connect-s3/index.html).

Add the following to `connector.properties` file for lakeFS support:

```properties
# Your lakeFS repository
s3.bucket.name=example-repo

# Your lakeFS S3 endpoint and credentials
store.url=https://lakefs.example.com
aws.access.key.id=AKIAIOSFODNN7EXAMPLE
aws.secret.access.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# main being the branch we want to write to
topics.dir=main/topics 
```

!!! example
    For usage examples, see the lakeFS [Kafka sample repo](https://github.com/treeverse/lakeFS-samples/tree/main/01_standalone_examples/kafka). 
