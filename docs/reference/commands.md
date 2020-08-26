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

The `lakectl` is distributed as a single binary, with no external dependencies - and is available for MacOS, Windows and Linux.

[Download lakectl](../downloads.md){: .btn .btn-green target="_blank"}


### Configuring credentials and API endpoint

Once you've installed the lakectl command, run:

```bash
$ lakectl config
Config file /home/janedoe/.lakectl.yaml will be used
Access key ID: AKIAIOSFODNN7EXAMPLE
Secret access key: ****************************************
Server endpoint URL: http://localhost:8000/api/v1
```

This will setup a `$HOME/.lakectl.yaml` file with the credentials and API endpoint you've supplied.
When setting up a new installation and creating initial credentials (see [Quick start](../quickstart/index.md)), the UI
will provide a link to download a preconfigured configuration file for you.


### Command Reference

##### `lakectl branch create`
````text
create a new branch in a repository

Usage:
  lakectl branch create [ref uri] [flags]

Flags:
  -h, --help            help for create
  -s, --source string   source branch uri

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl branch delete`
````text
delete a branch in a repository, along with its uncommitted changes (CAREFUL)

Usage:
  lakectl branch delete [branch uri] [flags]

Flags:
  -h, --help   help for delete
  -y, --sure   do not ask for confirmation

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl branch list`
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
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl branch revert`
````text
revert changes - there are four different ways to revert changes:
  1. revert to previous commit, set HEAD of branch to given commit - revert lakefs://myrepo@master --commit commitId
  2. revert all uncommitted changes (reset) - revert lakefs://myrepo@master
  3. revert uncommitted changes under specific path -	revert lakefs://myrepo@master --prefix path
  4. revert uncommitted changes for specific object - revert lakefs://myrepo@master --object path

Usage:
  lakectl branch revert [branch uri] [flags]

Flags:
      --commit string   commit ID to revert branch to
  -h, --help            help for revert
      --object string   path to object to be reverted
      --tree string     path to tree to be reverted

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl branch show`
````text
show branch metadata

Usage:
  lakectl branch show [branch uri] [flags]

Flags:
  -h, --help   help for show

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl commit`
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
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl config`
````text
Create/update local lakeFS configuration

Usage:
  lakectl config [flags]

Flags:
  -h, --help   help for config

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl diff`
````text
see the list of paths added/changed/removed in a branch or between two references (could be either commit hash or branch name)

Usage:
  lakectl diff [ref uri] <other ref uri> [flags]

Flags:
  -h, --help   help for diff

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl fs cat`
````text
dump content of object to stdout

Usage:
  lakectl fs cat [path uri] [flags]

Flags:
  -h, --help               help for cat
      --read-uncommitted   read uncommitted data (default true)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl fs ls`
````text
list entries under a given tree

Usage:
  lakectl fs ls [path uri] [flags]

Flags:
  -h, --help               help for ls
      --read-uncommitted   read uncommitted data (default true)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl fs rm`
````text
delete object

Usage:
  lakectl fs rm [path uri] [flags]

Flags:
  -h, --help   help for rm

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl fs stat`
````text
view object metadata

Usage:
  lakectl fs stat [path uri] [flags]

Flags:
  -h, --help               help for stat
      --read-uncommitted   read uncommitted data (default true)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

````

##### `lakectl fs upload`
````text
upload a local file to the specified URI

Usage:
  lakectl fs upload [path uri] [flags]

Flags:
  -h, --help            help for upload
  -s, --source string   local file to upload, or "-" for stdin

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

````

##### `lakectl log`
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
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

````

##### `lakectl merge`
````text
merge & commit changes from source branch into destination branch

Usage:
  lakectl merge [flags]

Flags:
  -h, --help   help for merge

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl repo create`
````text
create a new repository

Usage:
  lakectl repo create  [repository uri] [bucket name] [flags]

Flags:
  -d, --default-branch string   the default branch of this repository (default "master")
  -h, --help                    help for create

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl repo delete`
````text
delete existing repository

Usage:
  lakectl repo delete [repository uri] [flags]

Flags:
  -h, --help   help for delete

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

````

##### `lakectl repo list`
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
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl show`
````text
See detailed information about an entity by ID (commit, user, etc)

Usage:
  lakectl show [repository uri] [flags]

Flags:
      --commit string   commit id to show
  -h, --help            help for show

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
````

##### `lakectl auth users create `
```text
create a user

Usage:
  lakectl auth users create [flags]

Flags:
  -h, --help        help for create
      --id string   user identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth users list `
```text
list users

Usage:
  lakectl auth users list [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth users delete `
```text
delete a user

Usage:
  lakectl auth users delete [flags]

Flags:
  -h, --help        help for delete
      --id string   user identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth users --id <userID> groups list `
```text
list groups for the given user

Usage:
  lakectl auth users groups list [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
      --id string      user identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```


##### `lakectl auth users --id <userID> credentials list `
```text
create user credentials

Usage:
  lakectl auth users credentials create [flags]

Flags:
  -h, --help        help for create
      --id string   user identifier (default: current user)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth users --id <userID> credentials create `
```text
create user credentials

Usage:
  lakectl auth users credentials create [flags]

Flags:
  -h, --help        help for create
      --id string   user identifier (default: current user)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth users --id <userID> credentials delete `
```text
delete user credentials

Usage:
  lakectl auth users credentials delete [flags]

Flags:
      --access-key-id string   access key ID to delete
  -h, --help                   help for delete
      --id string              user identifier (default: current user)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
```

##### `lakectl auth users --id <userID> policies list `
```text
list policies for the given user

Usage:
  lakectl auth users policies list [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
      --effective      list all distinct policies attached to the user, even through group memberships
  -h, --help           help for list
      --id string      user identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```


##### `lakectl auth users --id <userID> policies attach `
```text
attach a policy to a user

Usage:
  lakectl auth users policies attach [flags]

Flags:
  -h, --help            help for attach
      --id string       user identifier
      --policy string   policy identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```


##### `lakectl auth users --id <userID> policies detach`
```text
detach a policy from a user

Usage:
  lakectl auth users policies detach [flags]

Flags:
  -h, --help            help for detach
      --id string       user identifier
      --policy string   policy identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth groups list`
```text
list groups

Usage:
  lakectl auth groups list [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth groups create`
```text
create a group

Usage:
  lakectl auth groups create [flags]

Flags:
  -h, --help        help for create
      --id string   group identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)


```

##### `lakectl auth groups delete`
```text
delete a group

Usage:
  lakectl auth groups delete [flags]

Flags:
  -h, --help        help for delete
      --id string   group identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth groups --id <groupID> members list`
```text
list users in a group

Usage:
  lakectl auth groups members list [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
      --id string      group identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)


```

##### `lakectl auth groups --id <groupID> members add`
```text
add a user to a group

Usage:
  lakectl auth groups members add [flags]

Flags:
  -h, --help          help for add
      --id string     group identifier
      --user string   user identifier to add to the group

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)


```

##### `lakectl auth groups --id <groupID> members remove`
```text
remove a user from a group

Usage:
  lakectl auth groups members remove [flags]

Flags:
  -h, --help          help for remove
      --id string     group identifier
      --user string   user identifier to add to the group

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth groups --id <groupID> policies list`
```text
list policies for the given group

Usage:
  lakectl auth groups policies list [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
      --id string      group identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth groups --id <groupID> policies attach`
```text
attach a policy to a group

Usage:
  lakectl auth groups policies attach [flags]

Flags:
  -h, --help            help for attach
      --id string       user identifier
      --policy string   policy identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth groups --id <groupID> policies detach`
```text
detach a policy from a group

Usage:
  lakectl auth groups policies detach [flags]

Flags:
  -h, --help            help for detach
      --id string       user identifier
      --policy string   policy identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```


##### `lakectl auth policies create`
```text
create a policy

Usage:
  lakectl auth policies create [flags]

Flags:
  -h, --help                        help for create
      --id string                   policy identifier
      --statement-document string   JSON statement document path (or "-" for stdin)

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth policies delete`
```text
delete a policy

Usage:
  lakectl auth policies delete [flags]

Flags:
  -h, --help        help for delete
      --id string   policy identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth policies list`
```text
list policies

Usage:
  lakectl auth policies list [flags]

Flags:
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

##### `lakectl auth policies show`
```text
show a policy

Usage:
  lakectl auth policies show [flags]

Flags:
  -h, --help        help for show
      --id string   policy identifier

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

```

#### `lakectl metastore copy`
````text
copy or merge table. the destination table will point to the selected branch

Usage:
  lakectl metastore copy [flags]

Flags:
      --catalog-id string      Glue catalog ID
      --from-schema string     source schema name
      --from-table string      source table name
  -h, --help                   help for copy
      --metastore-uri string   Hive metastore URI
  -p, --partition strings      partition to copy
      --serde string           serde to set copy to  [default is  to-table]
      --to-branch string       lakeFS branch name
      --to-schema string       destination schema name [default is from-branch]
      --to-table string        destination table name [default is  from-table] 
      --type string            metastore type [hive, glue]

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

````

#### `lakectl metastore diff`
````text
show column and partition differences between two tables

Usage:
  lakectl metastore diff [flags]

Flags:
      --catalog-id string      Glue catalog ID
      --from-schema string     source schema name
      --from-table string      source table name
  -h, --help                   help for diff
      --metastore-uri string   Hive metastore URI
      --to-schema string       destination schema name 
      --to-table string        destination table name [default is from-table]
      --type string            metastore type [hive, glue]

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

````
#### `lakectl metastore create-symlink`
````text
create table with symlinks, and create the symlinks in s3 in order to access from external devices that could only access s3 directly (e.g athena)

Usage:
  lakectl metastore create-symlink [flags]

Flags:
      --branch string        lakeFS branch name
      --catalog-id string    Glue catalog ID
      --from-schema string   source schema name
      --from-table string    source table name
  -h, --help                 help for create-symlink
      --path string          path to table on lakeFS
      --repo string          lakeFS repository name
      --to-schema string     destination schema name
      --to-table string      destination table name

Global Flags:
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)

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
