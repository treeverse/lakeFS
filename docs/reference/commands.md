---
layout: default
title: Command (CLI) Reference
parent: Reference
nav_order: 3
has_children: false
---

# Commands (CLI) Reference
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

### Installing the lakectl command locally

The `lakectl` command is distributed as a single binary, with no external dependencies - and is available for MacOS, Windows and Linux.

[Download lakectl](https://github.com){: .btn .btn-green target="_blank"}


### Configuring credentials and API endpoint

Once you've installed the lakectl command, run:

```bash
$ lakectl config
Config file /home/janedoe/.lakectl.yaml will be used
Access key ID: AKIAJNYOQZSWBSSXURPQ
Secret access key: ****************************************
Server endpoint URL: http://localhost:8001/api/v1
```

This will setup a `$HOME/.lakectl.yaml` file with the credentials and API endpoint you've supplied.
When setting up a new installation and creating initial credentials (see [Quick start](../quickstart.md)), the UI
will provide a link to download a preconfigured configuration file for you.


### Command Reference



