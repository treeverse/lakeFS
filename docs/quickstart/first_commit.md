---
layout: default
title: Commit the Changes
parent: Quickstart
nav_order: 40
has_children: false
---

# Commit the Changes

## Install lakectl

lakeFS comes with its own native CLI client, `lakectl`. 
Most importantly, you can use it to perform git-like operations like committing, reverting and merging.

Follow the below tutorial video to get started with the CLI, or follow the instructions on this page.

[![IMAGE ALT TEXT](http://img.youtube.com/vi/8nO7RT411nA/0.jpg)](http://www.youtube.com/watch?v=8nO7RT411nA "Install the lakeFS CLI")

Here's how to get started with the CLI:

  1. Download the CLI binary:

     The CLI binary can be downloaded from the official [lakeFS releases](https://github.com/treeverse/lakeFS/releases) published on GitHub under "Assets". Unless you need a specific previous version of the CLI, it is recommended to download the most recently released version.

      ![Release Assets]({{ site.baseurl }}/assets/img/lakefs-release-asset.png)

     The Operating System of the computer being used determines whether you should pick the binary Asset compiled for a Windows, Linux, or Mac (Darwin). For Mac and Linux Operating Systems, the processor determines whether you should download the x64 or arm binary. 
  
  
  1. Once unzipped, inside the downloaded asset, you'll see a file named `lakectl`. It's recommended that you place this file somewhere in your PATH (this is OS dependant but for *NIX systems , `/usr/local/bin` is usually a safe bet). Once in your PATH, you'll be able to open a Terminal program and run lakectl commands!

  1. We recommend starting with `lakectl config` to configure the CLI to use the credentials created earlier:

     ```bash
     lakectl config
     # output:
     # Config file /home/janedoe/.lakectl.yaml will be used
     # Access key ID: AKIAJVHTOKZWGCD2QQYQ
     # Secret access key: ****************************************
     # Server endpoint URL: http://localhost:8000
     ```
   **Note** The first time you run a `lakectl` command, depending on your computer's security settings, you may need to respond to a prompt to allow the program to run. 

  1. To verify `lakectl` is properly configured, we can list the branches in our repository:

     ```bash
     lakectl branch list lakefs://example-repo
     # output:
     # +----------+------------------------------------------------------------------+
     # | REF NAME | COMMIT ID                                                        |
     # +----------+------------------------------------------------------------------+
     # | main     | a91f56a7e11be1348fc405053e5234e4af7d6da01ed02f3d9a8ba7b1f71499c8 |
     # +----------+------------------------------------------------------------------+
     ```
  
## Perform your first commit

Let's commit the file we've added in the previous section:

```
lakectl commit lakefs://example-repo/main -m 'added our first file!'
# output:
# Commit for branch "main" done.
# 
# ID: 901f7b21e1508e761642b142aea0ccf28451675199655381f65101ea230ebb87
# Timestamp: 2021-06-15 13:48:37 +0300 IDT
# Parents: a91f56a7e11be1348fc405053e5234e4af7d6da01ed02f3d9a8ba7b1f71499c8
```

And finally we can view the log to see the new commit:
```
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
