---
title: Get Started
description: Methods for getting started with lakeFS Enterprise
parent: lakeFS Enterprise
has_children: true
has_toc: false
nav_order: 200
---

# Get Started with lakeFS Enterprise

## Install

There are three ways to install lakeFS Enterprise:
* **Docker Quickstart Install**: Intended for testing and evaluating lakeFS Enterprise quickly, and is not designed for production systems.
* **Kubernetes Helm-based Quickstart Install**:
* **Production Deployments**: a Kubernetes base deployment that runs on...

## Log Collection

The recommended practice for collecting logs would be sending them to the container std (default configuration)
and letting an external service to collect them to a sink. An example for logs collector would be [fluentbit](https://fluentbit.io/)
that can collect container logs, format them and ship them to a target like S3.

There are 2 kinds of logs, regular logs like an API error or some event description used for debugging
and audit_logs that are describing a user action (i.e create branch).
The distinction between regular logs and audit_logs is in the boolean field log_audit.
lakeFS and fluffy share the same configuration structure under logging.* section in the config.

{: .warning }
> The log entries marked with `log_audit = true` are currently available in both the open-source version of lakeFS and lakeFS Enterprise. However, please be aware that these log entries are deprecated in the open source version and will be removed in future releases.

