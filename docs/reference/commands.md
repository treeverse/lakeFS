---
layout: default
title: Command (CLI) Reference
description: lakeFS comes with its own native CLI client. Here you can see the complete command reference.
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

## Installing the lakectl command locally

`lakectl` is distributed as a single binary, with no external dependencies - and is available for MacOS, Windows and Linux.

[Download lakectl](../downloads.md){: .btn .btn-green target="_blank"}


### Configuring credentials and API endpoint

Once you've installed the lakectl command, run:

```bash
lakectl config
# output:
# Config file /home/janedoe/.lakectl.yaml will be used
# Access key ID: AKIAIOSFODNN7EXAMPLE
# Secret access key: ****************************************
# Server endpoint URL: http://localhost:8000/api/v1
```

This will setup a `$HOME/.lakectl.yaml` file with the credentials and API endpoint you've supplied.
When setting up a new installation and creating initial credentials (see [Quick start](../quickstart/index.md)), the UI
will provide a link to download a preconfigured configuration file for you.

### lakectl

A cli tool to explore manage and work with lakeFS

#### Synopsis

lakeFS is data lake management solution, allowing Git-like semantics over common object stores

lakectl is a CLI tool allowing exploration and manipulation of a lakeFS environment

#### Options

```
  -c, --config string   config file (default is $HOME/.lakectl.yaml)
  -h, --help            help for lakectl
      --no-color        don't use fancy output colors (default when not attached to an interactive terminal)
```



### lakectl abuse

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

abuse a running lakeFS instance. See sub commands for more info.

#### Options

```
  -h, --help   help for abuse
```



### lakectl abuse create-branches

Create a lot of branches very quickly.

```
lakectl abuse create-branches <source ref uri> [flags]
```

#### Options

```
      --amount int             amount of things to do (default 1000000)
      --branch-prefix string   prefix to create branches under (default "abuse-")
      --clean-only             only clean up past runs
  -h, --help                   help for create-branches
      --parallelism int        amount of things to do in parallel (default 100)
```



### lakectl abuse help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type abuse help [path to command] for full details.

```
lakectl abuse help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl abuse random-read

Read keys from a file and generate random reads from the source ref for those keys.

```
lakectl abuse random-read <source ref uri> [flags]
```

#### Options

```
      --amount int         amount of reads to do (default 1000000)
      --from-file string   read keys from this file ("-" for stdin)
  -h, --help               help for random-read
      --parallelism int    amount of reads to do in parallel (default 100)
```



### lakectl actions

Manage Actions commands

#### Options

```
  -h, --help   help for actions
```



### lakectl actions help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type actions help [path to command] for full details.

```
lakectl actions help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl actions runs

Explore runs information

#### Options

```
  -h, --help   help for runs
```



### lakectl actions runs describe

Describe run results

#### Synopsis

Show information about the run and all the hooks that were executed as part of the run

```
lakectl actions runs describe [flags]
```

#### Examples

```
lakectl actions runs describe lakefs://<repository> <run_id>
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return, or '-1' for default (used for pagination) (default -1)
  -h, --help           help for describe
```



### lakectl actions runs help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type runs help [path to command] for full details.

```
lakectl actions runs help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl actions runs list

List runs

#### Synopsis

List all runs on a repository optional filter by branch or commit

```
lakectl actions runs list [flags]
```

#### Examples

```
lakectl actions runs list lakefs://<repository> [--branch <branch>] [--commit <commit_id>]
```

#### Options

```
      --after string    show results after this value (used for pagination)
      --amount int      how many results to return, or '-1' for default (used for pagination) (default -1)
      --branch string   show results for specific branch
      --commit string   show results for specific commit ID
  -h, --help            help for list
```



### lakectl auth

manage authentication and authorization

#### Synopsis

manage authentication and authorization including users, groups and policies

#### Options

```
  -h, --help   help for auth
```



### lakectl auth groups

manage groups

#### Options

```
  -h, --help   help for groups
```



### lakectl auth groups create

create a group

```
lakectl auth groups create [flags]
```

#### Options

```
  -h, --help        help for create
      --id string   group identifier
```



### lakectl auth groups delete

delete a group

```
lakectl auth groups delete [flags]
```

#### Options

```
  -h, --help        help for delete
      --id string   group identifier
```



### lakectl auth groups help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type groups help [path to command] for full details.

```
lakectl auth groups help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth groups list

list groups

```
lakectl auth groups list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
```



### lakectl auth groups members

manage group user memberships

#### Options

```
  -h, --help   help for members
```



### lakectl auth groups members add

add a user to a group

```
lakectl auth groups members add [flags]
```

#### Options

```
  -h, --help          help for add
      --id string     group identifier
      --user string   user identifier to add to the group
```



### lakectl auth groups members help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type members help [path to command] for full details.

```
lakectl auth groups members help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth groups members list

list users in a group

```
lakectl auth groups members list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
      --id string      group identifier
```



### lakectl auth groups members remove

remove a user from a group

```
lakectl auth groups members remove [flags]
```

#### Options

```
  -h, --help          help for remove
      --id string     group identifier
      --user string   user identifier to add to the group
```



### lakectl auth groups policies

manage group policies

#### Options

```
  -h, --help   help for policies
```



### lakectl auth groups policies attach

attach a policy to a group

```
lakectl auth groups policies attach [flags]
```

#### Options

```
  -h, --help            help for attach
      --id string       user identifier
      --policy string   policy identifier
```



### lakectl auth groups policies detach

detach a policy from a group

```
lakectl auth groups policies detach [flags]
```

#### Options

```
  -h, --help            help for detach
      --id string       user identifier
      --policy string   policy identifier
```



### lakectl auth groups policies help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth groups policies help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth groups policies list

list policies for the given group

```
lakectl auth groups policies list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
      --id string      group identifier
```



### lakectl auth help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type auth help [path to command] for full details.

```
lakectl auth help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth policies

manage policies

#### Options

```
  -h, --help   help for policies
```



### lakectl auth policies create

create a policy

```
lakectl auth policies create [flags]
```

#### Options

```
  -h, --help                        help for create
      --id string                   policy identifier
      --statement-document string   JSON statement document path (or "-" for stdin)
```



### lakectl auth policies delete

delete a policy

```
lakectl auth policies delete [flags]
```

#### Options

```
  -h, --help        help for delete
      --id string   policy identifier
```



### lakectl auth policies help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth policies help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth policies list

list policies

```
lakectl auth policies list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
```



### lakectl auth policies show

show a policy

```
lakectl auth policies show [flags]
```

#### Options

```
  -h, --help        help for show
      --id string   policy identifier
```



### lakectl auth users

manage users

#### Options

```
  -h, --help   help for users
```



### lakectl auth users create

create a user

```
lakectl auth users create [flags]
```

#### Options

```
  -h, --help        help for create
      --id string   user identifier
```



### lakectl auth users credentials

manage user credentials

#### Options

```
  -h, --help   help for credentials
```



### lakectl auth users credentials create

create user credentials

```
lakectl auth users credentials create [flags]
```

#### Options

```
  -h, --help        help for create
      --id string   user identifier (default: current user)
```



### lakectl auth users credentials delete

delete user credentials

```
lakectl auth users credentials delete [flags]
```

#### Options

```
      --access-key-id string   access key ID to delete
  -h, --help                   help for delete
      --id string              user identifier (default: current user)
```



### lakectl auth users credentials help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type credentials help [path to command] for full details.

```
lakectl auth users credentials help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth users credentials list

list user credentials

```
lakectl auth users credentials list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
      --id string      user identifier (default: current user)
```



### lakectl auth users delete

delete a user

```
lakectl auth users delete [flags]
```

#### Options

```
  -h, --help        help for delete
      --id string   user identifier
```



### lakectl auth users groups

manage user groups

#### Options

```
  -h, --help   help for groups
```



### lakectl auth users groups help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type groups help [path to command] for full details.

```
lakectl auth users groups help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth users groups list

list groups for the given user

```
lakectl auth users groups list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
      --id string      user identifier
```



### lakectl auth users help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type users help [path to command] for full details.

```
lakectl auth users help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth users list

list users

```
lakectl auth users list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
  -h, --help           help for list
```



### lakectl auth users policies

manage user policies

#### Options

```
  -h, --help   help for policies
```



### lakectl auth users policies attach

attach a policy to a user

```
lakectl auth users policies attach [flags]
```

#### Options

```
  -h, --help            help for attach
      --id string       user identifier
      --policy string   policy identifier
```



### lakectl auth users policies detach

detach a policy from a user

```
lakectl auth users policies detach [flags]
```

#### Options

```
  -h, --help            help for detach
      --id string       user identifier
      --policy string   policy identifier
```



### lakectl auth users policies help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth users policies help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl auth users policies list

list policies for the given user

```
lakectl auth users policies list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return (default 100)
      --effective      list all distinct policies attached to the user, even through group memberships
  -h, --help           help for list
      --id string      user identifier
```



### lakectl branch

create and manage branches within a repository

#### Synopsis

Create delete and list branches within a lakeFS repository

#### Options

```
  -h, --help   help for branch
```



### lakectl branch create

create a new branch in a repository

```
lakectl branch create <ref uri> [flags]
```

#### Options

```
  -h, --help            help for create
  -s, --source string   source branch uri
```



### lakectl branch delete

delete a branch in a repository, along with its uncommitted changes (CAREFUL)

```
lakectl branch delete <branch uri> [flags]
```

#### Options

```
  -h, --help   help for delete
  -y, --yes    Automatically say yes to all confirmations
```



### lakectl branch help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type branch help [path to command] for full details.

```
lakectl branch help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl branch list

list branches in a repository

```
lakectl branch list <repository uri> [flags]
```

#### Examples

```
lakectl branch list lakefs://<repository>
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return, or '-1' for default (used for pagination) (default -1)
  -h, --help           help for list
```



### lakectl branch reset

reset changes to specified commit, or reset uncommitted changes - all changes, or by path

#### Synopsis

reset changes.  There are four different ways to reset changes:
  1. reset to previous commit, set HEAD of branch to given commit - reset lakefs://myrepo@master --commit commitId
  2. reset all uncommitted changes - reset lakefs://myrepo@master 
  3. reset uncommitted changes under specific path -	reset lakefs://myrepo@master --prefix path
  4. reset uncommitted changes for specific object - reset lakefs://myrepo@master --object path

```
lakectl branch reset <branch uri> [flags]
```

#### Options

```
      --commit string   commit ID to reset branch to
  -h, --help            help for reset
      --object string   path to object to be reset
      --prefix string   prefix of the objects to be reset
  -y, --yes             Automatically say yes to all confirmations
```



### lakectl branch revert

given a commit, record a new commit to reverse the effect of this commit

```
lakectl branch revert <branch uri> <commit ref to revert> [flags]
```

#### Options

```
  -h, --help                help for revert
  -m, --parent-number int   the parent number (starting from 1) of the mainline. The revert will reverse the change relative to the specified parent.
  -y, --yes                 Automatically say yes to all confirmations
```



### lakectl branch show

show branch latest commit reference

```
lakectl branch show <branch uri> [flags]
```

#### Options

```
  -h, --help   help for show
```



### lakectl cat-hook-output

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

Cat actions hook output

```
lakectl cat-hook-output [flags]
```

#### Examples

```
lakectl cat-hook-output lakefs://<repository> <run_id> <run_hook_id>
```

#### Options

```
  -h, --help   help for cat-hook-output
```



### lakectl cat-sst

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

Explore lakeFS .sst files

```
lakectl cat-sst <sst-file> [flags]
```

#### Options

```
      --amount int    how many records to return, or -1 for all records (default -1)
  -f, --file string   path to an sstable file, or "-" for stdin
  -h, --help          help for cat-sst
```



### lakectl commit

commit changes on a given branch

```
lakectl commit <branch uri> [flags]
```

#### Options

```
  -h, --help             help for commit
  -m, --message string   commit message
      --meta strings     key value pair in the form of key=value
```



### lakectl completion

Generate completion script

#### Synopsis

To load completions:

Bash:

```sh
$ source <(lakectl completion bash)
```

To load completions for each session, execute once:
Linux:

```sh
$ lakectl completion bash > /etc/bash_completion.d/lakectl
```

MacOS:

```sh
$ lakectl completion bash > /usr/local/etc/bash_completion.d/lakectl
```

Zsh:

If shell completion is not already enabled in your environment you will need
to enable it.  You can execute the following once:

```sh
$ echo "autoload -U compinit; compinit" >> ~/.zshrc
```

To load completions for each session, execute once:
```sh
$ lakectl completion zsh > "${fpath[1]}/_lakectl"
```

You will need to start a new shell for this setup to take effect.

Fish:

```sh
$ lakectl completion fish | source
```

To load completions for each session, execute once:

```sh
$ lakectl completion fish > ~/.config/fish/completions/lakectl.fish
```



```
lakectl completion <bash|zsh|fish>
```

#### Options

```
  -h, --help   help for completion
```



### lakectl config

create/update local lakeFS configuration

```
lakectl config [flags]
```

#### Options

```
  -h, --help   help for config
```



### lakectl diff

diff between commits/hashes

#### Synopsis

see the list of paths added/changed/removed in a branch or between two references (could be either commit hash or branch name)

```
lakectl diff <ref uri> [other ref uri] [flags]
```

#### Options

```
  -h, --help   help for diff
```



### lakectl docs

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }



```
lakectl docs [outfile] [flags]
```

#### Options

```
  -h, --help   help for docs
```



### lakectl fs

view and manipulate objects

#### Options

```
  -h, --help   help for fs
```



### lakectl fs cat

dump content of object to stdout

```
lakectl fs cat <path uri> [flags]
```

#### Options

```
  -h, --help   help for cat
```



### lakectl fs help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type fs help [path to command] for full details.

```
lakectl fs help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl fs ls

list entries under a given tree

```
lakectl fs ls <path uri> [flags]
```

#### Options

```
  -h, --help   help for ls
```



### lakectl fs rm

delete object

```
lakectl fs rm <path uri> [flags]
```

#### Options

```
  -h, --help   help for rm
```



### lakectl fs stage

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

stage an object at the specified URI

```
lakectl fs stage <path uri> [flags]
```

#### Options

```
      --checksum string   Object MD5 checksum as a hexadecimal string
  -h, --help              help for stage
      --location string   fully qualified storage location (i.e. "s3://bucket/path/to/object")
      --size int          Object size in bytes
```



### lakectl fs stat

view object metadata

```
lakectl fs stat <path uri> [flags]
```

#### Options

```
  -h, --help   help for stat
```



### lakectl fs upload

upload a local file to the specified URI

```
lakectl fs upload <path uri> [flags]
```

#### Options

```
  -h, --help            help for upload
  -r, --recursive       recursively copy all files under local source
  -s, --source string   local file to upload, or "-" for stdin
```



### lakectl help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type lakectl help [path to command] for full details.

```
lakectl help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl log

show log of commits for the given branch

```
lakectl log <branch uri> [flags]
```

#### Options

```
      --after string         show results after this value (used for pagination)
      --amount int           how many results to return, or '-1' for default (used for pagination) (default -1)
  -h, --help                 help for log
      --show-meta-range-id   also show meta range ID
```



### lakectl merge

merge

#### Synopsis

merge & commit changes from source branch into destination branch

```
lakectl merge <source ref> <destination ref> [flags]
```

#### Options

```
  -h, --help   help for merge
```



### lakectl metastore

manage metastore commands

#### Options

```
  -h, --help   help for metastore
```



### lakectl metastore copy

copy or merge table

#### Synopsis

copy or merge table. the destination table will point to the selected branch

```
lakectl metastore copy [flags]
```

#### Options

```
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
```



### lakectl metastore create-symlink

create symlink table and data

#### Synopsis

create table with symlinks, and create the symlinks in s3 in order to access from external services that could only access s3 directly (e.g athena)

```
lakectl metastore create-symlink [flags]
```

#### Options

```
      --branch string        lakeFS branch name
      --catalog-id string    Glue catalog ID
      --from-schema string   source schema name
      --from-table string    source table name
  -h, --help                 help for create-symlink
      --path string          path to table on lakeFS
      --repo string          lakeFS repository name
      --to-schema string     destination schema name
      --to-table string      destination table name
```



### lakectl metastore diff

show column and partition differences between two tables

```
lakectl metastore diff [flags]
```

#### Options

```
      --catalog-id string      Glue catalog ID
      --from-schema string     source schema name
      --from-table string      source table name
  -h, --help                   help for diff
      --metastore-uri string   Hive metastore URI
      --to-schema string       destination schema name 
      --to-table string        destination table name [default is from-table]
      --type string            metastore type [hive, glue]
```



### lakectl metastore help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type metastore help [path to command] for full details.

```
lakectl metastore help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl refs-dump

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

dumps refs (branches, commits, tags) to the underlying object store

```
lakectl refs-dump <repository uri> [flags]
```

#### Options

```
  -h, --help   help for refs-dump
```



### lakectl refs-restore

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

restores refs (branches, commits, tags) from the underlying object store to a bare repository

#### Synopsis

restores refs (branches, commits, tags) from the underlying object store to a bare repository.

This command is expected to run on a bare repository (i.e. one created with 'lakectl repo create-bare').
Since a bare repo is expected, in case of transient failure, delete the repository and recreate it as bare and retry.

```
lakectl refs-restore <repository uri> [flags]
```

#### Examples

```
aws s3 cp s3://bucket/_lakefs/refs_manifest.json - | lakectl refs-load lakefs://my-bare-repository --manifest -
```

#### Options

```
  -h, --help                 help for refs-restore
      --manifest refs-dump   path to a refs manifest json file (as generated by refs-dump). Alternatively, use "-" to read from stdin
```



### lakectl repo

manage and explore repos

#### Options

```
  -h, --help   help for repo
```



### lakectl repo create

create a new repository 

```
lakectl repo create <repository uri> <storage namespace> [flags]
```

#### Options

```
  -d, --default-branch string   the default branch of this repository (default "master")
  -h, --help                    help for create
```



### lakectl repo create-bare

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

create a new repository with no initial branch or commit

```
lakectl repo create-bare <repository uri> <storage namespace> [flags]
```

#### Options

```
  -d, --default-branch string   the default branch name of this repository (will not be created) (default "master")
  -h, --help                    help for create-bare
```



### lakectl repo delete

delete existing repository

```
lakectl repo delete <repository uri> [flags]
```

#### Options

```
  -h, --help   help for delete
  -y, --yes    Automatically say yes to all confirmations
```



### lakectl repo help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type repo help [path to command] for full details.

```
lakectl repo help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl repo list

list repositories

```
lakectl repo list [flags]
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return, or '-1' for default (used for pagination) (default -1)
  -h, --help           help for list
```



### lakectl show

See detailed information about an entity by ID (commit, user, etc)

```
lakectl show <repository uri> [flags]
```

#### Options

```
      --commit string        commit ID to show
  -h, --help                 help for show
      --show-meta-range-id   when showing commits, also show meta range ID
```



### lakectl tag

create and manage tags within a repository

#### Synopsis

Create delete and list tags within a lakeFS repository

#### Options

```
  -h, --help   help for tag
```



### lakectl tag create

create a new tag in a repository

```
lakectl tag create <tag uri> <commit ref> [flags]
```

#### Options

```
  -h, --help   help for create
```



### lakectl tag delete

delete a tag from a repository

```
lakectl tag delete <tag uri> [flags]
```

#### Options

```
  -h, --help   help for delete
```



### lakectl tag help

Help about any command

#### Synopsis

Help provides help for any command in the application.
Simply type tag help [path to command] for full details.

```
lakectl tag help [command] [flags]
```

#### Options

```
  -h, --help   help for help
```



### lakectl tag list

list tags in a repository

```
lakectl tag list <repository uri> [flags]
```

#### Examples

```
lakectl tag list lakefs://<repository>
```

#### Options

```
      --after string   show results after this value (used for pagination)
      --amount int     how many results to return, or '-1' for default (used for pagination) (default -1)
  -h, --help           help for list
```



### lakectl tag show

show tag's commit reference

```
lakectl tag show <tag uri> [flags]
```

#### Options

```
  -h, --help   help for show
```



