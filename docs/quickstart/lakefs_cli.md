---
layout: default
title: Install lakeFS CLI
parent: Quickstart
nav_order: 40
has_children: false
---

# Install lakeFS CLI

  lakeFS comes with its own native CLI client. You can see the complete command reference [here](../reference/commands.md).

  The CLI is a great way to get started with lakeFS since it is a complete implementation of the lakeFS API.

  Here's how to get started with the CLI:

  1. Download the CLI binary:

     [Download lakectl](../index.md#downloads){: .btn .btn-green target="_blank"}
  
  
  1. It's recommended that you place it somewhere in your PATH (this is OS dependant but for *NIX systems , `/usr/local/bin` is usually a safe bet).
  1. configure the CLI to use the credentials you've created earlier:

     ```bash
     lakectl config
     # output:
     # Config file /home/janedoe/.lakectl.yaml will be used
     # Access key ID: AKIAJVHTOKZWGCD2QQYQ
     # Secret access key: ****************************************
     # Server endpoint URL: http://localhost:8000/api/v1
     ```

  1. Now that we've configured it, let's run a few sample commands:

     ```bash
     lakectl branch list lakefs://example-repo
     # output:
     # +----------+------------------------------------------------------------------+
     # | REF NAME | COMMIT ID                                                        |
     # +----------+------------------------------------------------------------------+
     # | main     | a91f56a7e11be1348fc405053e5234e4af7d6da01ed02f3d9a8ba7b1f71499c8 |
     # +----------+------------------------------------------------------------------+
     
     lakectl commit lakefs://example-repo/main -m 'added our first file!'
     # output:
     # Commit for branch "main" done.
     # 
     # ID: 901f7b21e1508e761642b142aea0ccf28451675199655381f65101ea230ebb87
     # Timestamp: 2021-06-15 13:48:37 +0300 IDT
     # Parents: a91f56a7e11be1348fc405053e5234e4af7d6da01ed02f3d9a8ba7b1f71499c8
  
     lakectl log lakefs://example-repo/main
     # output:  
     # commit 901f7b21e1508e761642b142aea0ccf28451675199655381f65101ea230ebb87
     # Author: Example User <user@example.com>
     # Date: 2021-06-15 13:48:37 +0300 IDT
       
           added our first file!
       
     ```

     **Note** lakeFS version <= v0.33.1 uses '@' (instead of '/') as separator between repository and branch.

### Next steps

Once you're ready to test lakeFS with a real workflow, it's time to [deploy lakeFS to AWS](../deploy/index.md).
