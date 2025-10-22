---
title: Python - Getting Started with High-Level SDK
description: Install and configure the lakeFS Python SDK for basic usage
---

# Getting Started with High-Level SDK

The High-Level SDK provides a Pythonic interface to lakeFS. This page covers installation, initialization, and configuration of the SDK.

## Installation

Install the High-Level SDK using pip:

```shell
pip install lakefs
```

Or upgrade to the latest version:

```shell
pip install --upgrade lakefs
```

## Initialization

The High-Level SDK by default will try to collect authentication parameters from the environment and attempt to create a default client. When working in an environment where **lakectl** is configured, it is not necessary to instantiate a lakeFS client or provide it for creating the lakeFS objects.

In case no authentication parameters exist, it is also possible to explicitly create a lakeFS client.

### Basic Client Initialization

Here's how to instantiate a client:

```python
from lakefs.client import Client

clt = Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)
```

!!! info
    See [here](../security/external-principals-aws.md#login-with-python) for instructions on how to log in with Python using your AWS role. This is applicable for enterprise users.

### SSL Configuration

You can use TLS with a CA that is not trusted on the host by configuring the client with a CA cert bundle file. It should contain concatenated CA certificates in PEM format:

```python
clt = Client(
    host="https://lakefs.example.io",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    # Customize the CA certificates used to verify the peer.
    ssl_ca_cert="path/to/concatenated_CA_certificates.PEM",
)
```

For connecting to a secure endpoint without verification (for test environments):

```python
clt = Client(
    host="https://lakefs.example.io",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    verify_ssl=False,
)
```

!!! warning
    This setting allows well-known "man-in-the-middle", impersonation, and credential stealing attacks. Never use this in any production setting.

### Proxy Configuration

To enable communication via proxies, add a proxy configuration:

```python
clt = Client(
    host="https://lakefs.example.io",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    ssl_ca_cert="(if needed)",
    proxy="<proxy server URL>",
)
```

## Next Steps

Once you have initialized the SDK, explore the different operations:

- **[Branches & Merging](./python-versioning-branches.md)** - Work with branches for feature development and experimentation
- **[Tags & Snapshots](./python-versioning-tags.md)** - Mark important data versions with immutable tags
- **[References & Commits](./python-references-commits.md)** - Navigate commit history and track lineage
- **[Transactions](./python-transactions.md)** - Perform atomic operations
- **[Data Operations](./python-data-operations.md)** - Upload, download, and manage objects
