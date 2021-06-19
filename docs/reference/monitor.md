---
layout: default
title: Monitoring using Prometheus
description: >-
  Users looking to monitor their lakeFS instances can point Prometheus
  configuration to scrape data from this endpoint. This guide explains how to
  setup
parent: Reference
nav_order: 30
has_children: false
redirect_from: ../deploying-aws/monitor.md
---

# Monitoring using Prometheus

{: .no\_toc }

{: .pb-3 }

## Table of contents

{: .no\_toc .text-delta }

1. TOC

   {:toc}

## Example prometheus.yml

lakeFS exposes metrics through the same port used by the lakeFS service, using the standard `/metrics` path. An example `prometheus.yml` could look like this:

```yaml
scrape_configs:
- job_name: lakeFS
  scrape_interval: 10s
  metrics_path: /metrics
  static_configs:
  - targets:
    - lakefs.example.com:8000
```

## Metrics exposed by lakeFS

By default, Prometheus exports metrics with OS process information like memory and CPU. It also includes Go-specific metrics like details about GC and number of goroutines. You can learn about these default metrics in this [post](https://povilasv.me/prometheus-go-metrics/){: target="\_blank" }.

In addition, lakeFS exposes the following metrics to help monitor your deployment:

\| Name in Prometheus \| Description \| Labels  
\| api_requests\_total \|_ [_lakeFS API_](api.md) _requests \(counter\)\| **code**: http status_  
_**method**: http method  
\| api\_request\_duration\_seconds \| Durations of lakeFS API requests \(histogram\)\|_   
_**operation**: name of API operation_  
_**code**: http status  
\| gateway\_request\_duration\_seconds \| lakeFS_ [_S3-compatible endpoint_](s3.md) _request \(histogram\)\|_   
_**operation**: name of gateway operation_  
_**code**: http status  
\| s3\_operation\_duration\_seconds \| Outgoing S3 operations \(histogram\)\|_   
_**operation**: operation name_  
_**error**: "true" if error, "false" otherwise \| gs\_operation\_duration\_seconds \| Outgoing Google Storage operations \(histogram\)\|_   
_**operation**: operation name_  
_**error**: "true" if error, "false" otherwise \| azure\_operation\_duration\_seconds \| Outgoing Azure storage operations \(histogram\)\|_   
_**operation**: operation name_  
_**error**: "true" if error, "false" otherwise \| go\_sql\_stats_\* \| [Go DB stats](https://golang.org/pkg/database/sql/#DB.Stats){: target="\_blank" } metrics have this prefix.  
[dlmiddlecote/sqlstats](https://github.com/dlmiddlecote/sqlstats){: target="\_blank" } is used to expose them.\|

## Example queries

**Note:** when using Prometheus functions like [rate](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate){: target="\_blank"} or [increase](https://prometheus.io/docs/prometheus/latest/querying/functions/#increase){: target="\_blank"}, results are extrapolated and may not be exact. {: .note}

### 99th percentile of API request latencies

```text
sum by (operation)(histogram_quantile(0.99, rate(api_request_duration_seconds_bucket[1m])))
```

### 50th percentile of S3-compatible API latencies

```text
sum by (operation)(histogram_quantile(0.5, rate(gateway_request_duration_seconds_bucket[1m])))
```

### Number of errors in outgoing S3 requests

```text
sum by (operation) (increase(s3_operation_duration_seconds_count{error="true"}[1m]))
```

### Number of open connections to the database

```text
go_sql_stats_connections_open
```

### Example Grafana dashboard

[![Grafana dashboard example](../../.gitbook/assets/grafana.png)](https://github.com/treeverse/lakeFS/tree/9d35f14eba038648902a59dbb091b27590525e87/docs/assets/img/grafana.png){: target="\_blank" }

