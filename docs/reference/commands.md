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

##### lakectl branch create

````text
create a new branch in a repository

Usage:
  lakectl branch create [ref uri] [flags]

Flags:
  -h, --help            help for create
  -s, --source string   source branch uri

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl branch delete

````text
delete a branch in a repository, along with its uncommitted changes (CAREFUL)

Usage:
  lakectl branch delete [branch uri] [flags]

Flags:
  -h, --help   help for delete
  -y, --sure   do not ask for confirmation

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl branch list

````text
list branches in a repository

Usage:
  lakectl branch list [repository uri] [flags]

Examples:
lakectl branch list lakefs://myrepo

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return, or-1 for all results (used for pagination) (default -1)
  -h, --help           help for list

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl branch revert

````text
revert changes: there are four different ways to revert changes:
				1. revert to previous commit, set HEAD of branch to given commit -  revert lakefs://myrepo@master --commit commitId
				2. revert all uncommitted changes (reset) -  revert lakefs://myrepo@master 
				3. revert uncommitted changes under specific path -	revert lakefs://myrepo@master  --tree path
				4. revert uncommited changes for specific object  - revert lakefs://myrepo@master  --object path

Usage:
  lakectl branch revert [branch uri] [flags]

Flags:
      --commit string   commit ID to revert branch to
  -h, --help            help for revert
      --object string   path to object to be reverted
      --tree string     path to tree to be reverted

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl branch show

````text
show branch metadata

Usage:
  lakectl branch show [branch uri] [flags]

Flags:
  -h, --help   help for show

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl commit

````text
commit changes on a given branch

Usage:
  lakectl commit [branch uri] [flags]

Flags:
  -h, --help             help for commit
  -m, --message string   commit message
      --meta strings     key value pair in the form of key=value

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl config

````text
Create/update local lakeFS configuration

Usage:
  lakectl config [flags]

Flags:
  -h, --help   help for config

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl diff

````text
see the list of paths added/changed/removed in a branch or between two references (could be either commit hash or branch name)

Usage:
  lakectl diff [ref uri] <other ref uri> [flags]

Flags:
  -h, --help   help for diff

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl fs cat

````text
dump content of object to stdout

Usage:
  lakectl fs cat [path uri] [flags]

Flags:
  -h, --help               help for cat
      --read-uncommitted   read uncommitted data (default true)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl fs ls

````text
list entries under a given tree

Usage:
  lakectl fs ls [path uri] [flags]

Flags:
  -h, --help               help for ls
      --read-uncommitted   read uncommitted data (default true)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl fs rm

````text
delete object

Usage:
  lakectl fs rm [path uri] [flags]

Flags:
  -h, --help   help for rm

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl fs stat

````text
view object metadata

Usage:
  lakectl fs stat [path uri] [flags]

Flags:
  -h, --help               help for stat
      --read-uncommitted   read uncommitted data (default true)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)

````

##### lakectl fs upload

````text
upload a local file to the specified URI

Usage:
  lakectl fs upload [path uri] [flags]

Flags:
  -h, --help            help for upload
  -s, --source string   local file to upload, or "-" for stdin

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)

````

##### lakectl log

````text
show log of commits for the given branch

Usage:
  lakectl log [branch uri] [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return, or-1 for all results (used for pagination) (default -1)
  -h, --help           help for log

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)

````

##### lakectl merge

````text
merge & commit changes from source branch into destination branch

Usage:
  lakectl merge [flags]

Flags:
  -h, --help   help for merge

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl repo create

````text
create a new repository

Usage:
  lakectl repo create  [repository uri] [bucket name] [flags]

Flags:
  -d, --default-branch string   the default branch of this repository (default "master")
  -h, --help                    help for create

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl repo delete

````text
delete existing repository

Usage:
  lakectl repo delete [repository uri] [flags]

Flags:
  -h, --help   help for delete

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)

````

##### lakectl repo list

````text
list repositories

Usage:
  lakectl repo list [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return, or-1 for all results (used for pagination) (default -1)
  -h, --help           help for list

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

##### lakectl show

````text
See detailed information about an entity by ID (commit, user, etc)

Usage:
  lakectl show [repository uri] [flags]

Flags:
      --commit string   commit id to show
  -h, --help            help for show

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        use fancy output colors (ignored when not attached to an interactive terminal)
````

### lakeFS URI pattern

Different CLI and UI operations use `lakefs://` URIs.  
These are resource identifiers used to indicate a certain resource within the system (repository, branch/commit id, object).

Their pattern is:

```text
Repository URI: lakefs://<repository_id>
Commit URI: lakefs://<repository_id>@<ref_id>
Branch URI: lakefs://<repository_id>@<ref_id>
Object URI: lakefs://<repository_id>@<ref_id>/<object_path>

ref_id = either a commit ID or a branch ID.
    lakeFS supports using them interchangeably where it makes sense to do so
```
