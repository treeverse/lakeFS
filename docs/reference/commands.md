---
layout: default
title: Command (CLI) Reference
description: lakeFS comes with its own native CLI client. Here you can see the complete command reference.
parent: Reference
nav_order: 3
has_children: false
---

# Commands (CLI) Reference
{:.no_toc}

{% include toc.html %}

## Installing the lakectl command locally

`lakectl` is distributed as a single binary, with no external dependencies - and is available for MacOS, Windows and Linux.

[Download lakectl](../index.md#downloads){: .btn .btn-green target="_blank"}


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

`lakectl` configuration items can each be controlled by an environment variable. The variable name will have a prefix of
*LAKECTL_*, followed by the name of the configuration, replacing every '.' with a '_'. Example: `LAKECTL_SERVER_ENDPOINT_URL` 
controls `server.endpoint_url`.
### lakectl

A cli tool to explore manage and work with lakeFS

#### Synopsis
{:.no_toc}

lakeFS is data lake management solution, allowing Git-like semantics over common object stores

lakectl is a CLI tool allowing exploration and manipulation of a lakeFS environment

#### Options
{:.no_toc}

```
      --base-uri string      base URI used for lakeFS address parse
  -c, --config string        config file (default is $HOME/.lakectl.yaml)
  -h, --help                 help for lakectl
      --log-format string    set logging output format
      --log-level string     set logging level (default "none")
      --log-output strings   set logging output(s)
      --no-color             don't use fancy output colors (default when not attached to an interactive terminal)
      --verbose              run in verbose mode
```

**note:** The `base-uri` option can be controlled with the `LAKECTL_BASE_URI` environment variable.
{: .note .note-warning }

#### Example usage
{:.no_toc}

```shell
$ export LAKECTL_BASE_URI="lakefs://my-repo/my-branch"
# Once set, use relative lakefs uri's:
$ lakectl fs ls /path
```

### lakectl abuse

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

Abuse a running lakeFS instance. See sub commands for more info.

#### Options
{:.no_toc}

```
  -h, --help   help for abuse
```



### lakectl abuse commit

Commits to the source ref repeatedly

```
lakectl abuse commit <source ref uri> [flags]
```

#### Options
{:.no_toc}

```
      --amount int     amount of commits to do (default 100)
      --gap duration   duration to wait between commits (default 2s)
  -h, --help           help for commit
```



### lakectl abuse create-branches

Create a lot of branches very quickly.

```
lakectl abuse create-branches <source ref uri> [flags]
```

#### Options
{:.no_toc}

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
{:.no_toc}

Help provides help for any command in the application.
Simply type abuse help [path to command] for full details.

```
lakectl abuse help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl abuse link-same-object

Link the same object in parallel.

```
lakectl abuse link-same-object <source ref uri> [flags]
```

#### Options
{:.no_toc}

```
      --amount int        amount of link object to do (default 1000000)
  -h, --help              help for link-same-object
      --key string        key used for the test (default "linked-object")
      --parallelism int   amount of link object to do in parallel (default 100)
```



### lakectl abuse list

List from the source ref

```
lakectl abuse list <source ref uri> [flags]
```

#### Options
{:.no_toc}

```
      --amount int        amount of lists to do (default 1000000)
  -h, --help              help for list
      --parallelism int   amount of lists to do in parallel (default 100)
      --prefix string     prefix to list under (default "abuse/")
```



### lakectl abuse random-read

Read keys from a file and generate random reads from the source ref for those keys.

```
lakectl abuse random-read <source ref uri> [flags]
```

#### Options
{:.no_toc}

```
      --amount int         amount of reads to do (default 1000000)
      --from-file string   read keys from this file ("-" for stdin)
  -h, --help               help for random-read
      --parallelism int    amount of reads to do in parallel (default 100)
```



### lakectl abuse random-write

Generate random writes to the source branch

```
lakectl abuse random-write <source branch uri> [flags]
```

#### Options
{:.no_toc}

```
      --amount int        amount of writes to do (default 1000000)
  -h, --help              help for random-write
      --parallelism int   amount of writes to do in parallel (default 100)
      --prefix string     prefix to create paths under (default "abuse/")
```



### lakectl actions

Manage Actions commands

#### Options
{:.no_toc}

```
  -h, --help   help for actions
```



### lakectl actions help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type actions help [path to command] for full details.

```
lakectl actions help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl actions runs

Explore runs information

#### Options
{:.no_toc}

```
  -h, --help   help for runs
```



### lakectl actions runs describe

Describe run results

#### Synopsis
{:.no_toc}

Show information about the run and all the hooks that were executed as part of the run

```
lakectl actions runs describe [flags]
```

#### Examples
{:.no_toc}

```
lakectl actions runs describe lakefs://<repository> <run_id>
```

#### Options
{:.no_toc}

```
      --after string   show results after this value (used for pagination)
      --amount int     number of results to return. By default, all results are returned.
  -h, --help           help for describe
```



### lakectl actions runs help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type runs help [path to command] for full details.

```
lakectl actions runs help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl actions runs list

List runs

#### Synopsis
{:.no_toc}

List all runs on a repository optional filter by branch or commit

```
lakectl actions runs list [flags]
```

#### Examples
{:.no_toc}

```
lakectl actions runs list lakefs://<repository> [--branch <branch>] [--commit <commit_id>]
```

#### Options
{:.no_toc}

```
      --after string    show results after this value (used for pagination)
      --amount int      number of results to return (default 100)
      --branch string   show results for specific branch
      --commit string   show results for specific commit ID
  -h, --help            help for list
```



### lakectl actions validate

Validate action file

#### Synopsis
{:.no_toc}

Tries to parse the input action file as lakeFS action file

```
lakectl actions validate [flags]
```

#### Examples
{:.no_toc}

```
lakectl actions validate <path>
```

#### Options
{:.no_toc}

```
  -h, --help   help for validate
```



### lakectl annotate

List entries under a given path, annotating each with the latest modifying commit

```
lakectl annotate <path uri> [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for annotate
  -r, --recursive   recursively annotate all entries under a given path or prefix
```



### lakectl auth

Manage authentication and authorization

#### Synopsis
{:.no_toc}

manage authentication and authorization including users, groups and policies

#### Options
{:.no_toc}

```
  -h, --help   help for auth
```



### lakectl auth groups

Manage groups

#### Options
{:.no_toc}

```
  -h, --help   help for groups
```



### lakectl auth groups create

Create a group

```
lakectl auth groups create [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for create
      --id string   Group identifier
```



### lakectl auth groups delete

Delete a group

```
lakectl auth groups delete [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for delete
      --id string   Group identifier
```



### lakectl auth groups help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type groups help [path to command] for full details.

```
lakectl auth groups help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth groups list

List groups

```
lakectl auth groups list [flags]
```

#### Options
{:.no_toc}

```
      --amount int     how many results to return (default 100)
      --after string   show results after this value (used for pagination)
  -h, --help           help for list
```



### lakectl auth groups members

Manage group user memberships

#### Options
{:.no_toc}

```
  -h, --help   help for members
```



### lakectl auth groups members add

Add a user to a group

```
lakectl auth groups members add [flags]
```

#### Options
{:.no_toc}

```
  -h, --help          help for add
      --id string     Group identifier
      --user string   Username (email for password-based users, default: current user)
```



### lakectl auth groups members help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type members help [path to command] for full details.

```
lakectl auth groups members help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth groups members list

List users in a group

```
lakectl auth groups members list [flags]
```

#### Options
{:.no_toc}

```
      --id string      Group identifier
      --amount int     how many results to return (default 100)
      --after string   show results after this value (used for pagination)
  -h, --help           help for list
```



### lakectl auth groups members remove

Remove a user from a group

```
lakectl auth groups members remove [flags]
```

#### Options
{:.no_toc}

```
  -h, --help          help for remove
      --id string     Group identifier
      --user string   Username (email for password-based users, default: current user)
```



### lakectl auth groups policies

Manage group policies

#### Options
{:.no_toc}

```
  -h, --help   help for policies
```



### lakectl auth groups policies attach

Attach a policy to a group

```
lakectl auth groups policies attach [flags]
```

#### Options
{:.no_toc}

```
  -h, --help            help for attach
      --id string       User identifier
      --policy string   Policy identifier
```



### lakectl auth groups policies detach

Detach a policy from a group

```
lakectl auth groups policies detach [flags]
```

#### Options
{:.no_toc}

```
  -h, --help            help for detach
      --id string       User identifier
      --policy string   Policy identifier
```



### lakectl auth groups policies help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth groups policies help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth groups policies list

List policies for the given group

```
lakectl auth groups policies list [flags]
```

#### Options
{:.no_toc}

```
      --id string      Group identifier
      --amount int     how many results to return (default 100)
      --after string   show results after this value (used for pagination)
  -h, --help           help for list
```



### lakectl auth help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type auth help [path to command] for full details.

```
lakectl auth help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth policies

Manage policies

#### Options
{:.no_toc}

```
  -h, --help   help for policies
```



### lakectl auth policies create

Create a policy

```
lakectl auth policies create [flags]
```

#### Options
{:.no_toc}

```
  -h, --help                        help for create
      --id string                   Policy identifier
      --statement-document string   JSON statement document path (or "-" for stdin)
```



### lakectl auth policies delete

Delete a policy

```
lakectl auth policies delete [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for delete
      --id string   Policy identifier
```



### lakectl auth policies help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth policies help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth policies list

List policies

```
lakectl auth policies list [flags]
```

#### Options
{:.no_toc}

```
      --amount int     how many results to return (default 100)
      --after string   show results after this value (used for pagination)
  -h, --help           help for list
```



### lakectl auth policies show

Show a policy

```
lakectl auth policies show [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for show
      --id string   Policy identifier
```



### lakectl auth users

Manage users

#### Options
{:.no_toc}

```
  -h, --help   help for users
```



### lakectl auth users create

Create a user

```
lakectl auth users create [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for create
      --id string   Username
```



### lakectl auth users credentials

Manage user credentials

#### Options
{:.no_toc}

```
  -h, --help   help for credentials
```



### lakectl auth users credentials create

Create user credentials

```
lakectl auth users credentials create [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for create
      --id string   Username (email for password-based users, default: current user)
```



### lakectl auth users credentials delete

Delete user credentials

```
lakectl auth users credentials delete [flags]
```

#### Options
{:.no_toc}

```
      --access-key-id string   Access key ID to delete
  -h, --help                   help for delete
      --id string              Username (email for password-based users, default: current user)
```



### lakectl auth users credentials help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type credentials help [path to command] for full details.

```
lakectl auth users credentials help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth users credentials list

List user credentials

```
lakectl auth users credentials list [flags]
```

#### Options
{:.no_toc}

```
      --id string      Username (email for password-based users, default: current user)
      --amount int     how many results to return (default 100)
      --after string   show results after this value (used for pagination)
  -h, --help           help for list
```



### lakectl auth users delete

Delete a user

```
lakectl auth users delete [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for delete
      --id string   Username (email for password-based users)
```



### lakectl auth users groups

Manage user groups

#### Options
{:.no_toc}

```
  -h, --help   help for groups
```



### lakectl auth users groups help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type groups help [path to command] for full details.

```
lakectl auth users groups help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth users groups list

List groups for the given user

```
lakectl auth users groups list [flags]
```

#### Options
{:.no_toc}

```
      --id string      Username (email for password-based users)
      --amount int     how many results to return (default 100)
      --after string   show results after this value (used for pagination)
  -h, --help           help for list
```



### lakectl auth users help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type users help [path to command] for full details.

```
lakectl auth users help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth users list

List users

```
lakectl auth users list [flags]
```

#### Options
{:.no_toc}

```
      --amount int     how many results to return (default 100)
      --after string   show results after this value (used for pagination)
  -h, --help           help for list
```



### lakectl auth users policies

Manage user policies

#### Options
{:.no_toc}

```
  -h, --help   help for policies
```



### lakectl auth users policies attach

Attach a policy to a user

```
lakectl auth users policies attach [flags]
```

#### Options
{:.no_toc}

```
  -h, --help            help for attach
      --id string       Username (email for password-based users)
      --policy string   Policy identifier
```



### lakectl auth users policies detach

Detach a policy from a user

```
lakectl auth users policies detach [flags]
```

#### Options
{:.no_toc}

```
  -h, --help            help for detach
      --id string       Username (email for password-based users)
      --policy string   Policy identifier
```



### lakectl auth users policies help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth users policies help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl auth users policies list

List policies for the given user

```
lakectl auth users policies list [flags]
```

#### Options
{:.no_toc}

```
      --effective      List all distinct policies attached to the user, including by group memberships
      --id string      Username (email for password-based users)
      --amount int     how many results to return (default 100)
      --after string   show results after this value (used for pagination)
  -h, --help           help for list
```



### lakectl branch

Create and manage branches within a repository

#### Synopsis
{:.no_toc}

Create delete and list branches within a lakeFS repository

#### Options
{:.no_toc}

```
  -h, --help   help for branch
```



### lakectl branch create

Create a new branch in a repository

```
lakectl branch create <branch uri> -s <source ref uri> [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch create lakefs://example-repo/new-branch -s lakefs://example-repo/main
```

#### Options
{:.no_toc}

```
  -h, --help            help for create
  -s, --source string   source branch uri
```



### lakectl branch delete

Delete a branch in a repository, along with its uncommitted changes (CAREFUL)

```
lakectl branch delete <branch uri> [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch delete lakefs://example-repo/example-branch
```

#### Options
{:.no_toc}

```
  -h, --help   help for delete
  -y, --yes    Automatically say yes to all confirmations
```



### lakectl branch help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type branch help [path to command] for full details.

```
lakectl branch help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl branch list

List branches in a repository

```
lakectl branch list <repository uri> [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch list lakefs://<repository>
```

#### Options
{:.no_toc}

```
      --after string   show results after this value (used for pagination)
      --amount int     number of results to return (default 100)
  -h, --help           help for list
```



### lakectl branch reset

Reset uncommitted changes - all of them, or by path

#### Synopsis
{:.no_toc}

reset changes.  There are four different ways to reset changes:
  1. reset all uncommitted changes - reset lakefs://myrepo/main 
  2. reset uncommitted changes under specific path - reset lakefs://myrepo/main --prefix path
  3. reset uncommitted changes for specific object - reset lakefs://myrepo/main --object path

```
lakectl branch reset <branch uri> [--prefix|--object] [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch reset lakefs://example-repo/example-branch
```

#### Options
{:.no_toc}

```
  -h, --help            help for reset
      --object string   path to object to be reset
      --prefix string   prefix of the objects to be reset
  -y, --yes             Automatically say yes to all confirmations
```



### lakectl branch revert

Given a commit, record a new commit to reverse the effect of this commit

#### Synopsis
{:.no_toc}

The commits will be reverted in left-to-right order

```
lakectl branch revert <branch uri> <commit ref to revert> [<more commits>...] [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch revert lakefs://example-repo/example-branch commitA
	          Revert the changes done by commitA in example-branch
		      branch revert lakefs://example-repo/example-branch HEAD~1 HEAD~2 HEAD~3
		      Revert the changes done by the second last commit to the fourth last commit in example-branch
```

#### Options
{:.no_toc}

```
  -h, --help                help for revert
  -m, --parent-number int   the parent number (starting from 1) of the mainline. The revert will reverse the change relative to the specified parent.
  -y, --yes                 Automatically say yes to all confirmations
```



### lakectl branch show

Show branch latest commit reference

```
lakectl branch show <branch uri> [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch show lakefs://example-repo/example-branch
```

#### Options
{:.no_toc}

```
  -h, --help   help for show
```



### lakectl branch-protect

Create and manage branch protection rules

#### Synopsis
{:.no_toc}

Define branch protection rules to prevent direct changes. Changes to protected branches can only be done by merging from other branches.

#### Options
{:.no_toc}

```
  -h, --help   help for branch-protect
```



### lakectl branch-protect add

Add a branch protection rule

#### Synopsis
{:.no_toc}

Add a branch protection rule for a given branch name pattern

```
lakectl branch-protect add <repo uri> <pattern> [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch-protect add lakefs://<repository> 'stable_*'
```

#### Options
{:.no_toc}

```
  -h, --help   help for add
```



### lakectl branch-protect delete

Delete a branch protection rule

#### Synopsis
{:.no_toc}

Delete a branch protection rule for a given branch name pattern

```
lakectl branch-protect delete <repo uri> <pattern> [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch-protect delete lakefs://<repository> stable_*
```

#### Options
{:.no_toc}

```
  -h, --help   help for delete
```



### lakectl branch-protect help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type branch-protect help [path to command] for full details.

```
lakectl branch-protect help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl branch-protect list

List all branch protection rules

```
lakectl branch-protect list <repo uri> [flags]
```

#### Examples
{:.no_toc}

```
lakectl branch-protect list lakefs://<repository>
```

#### Options
{:.no_toc}

```
  -h, --help   help for list
```



### lakectl cat-hook-output

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

Cat actions hook output

```
lakectl cat-hook-output [flags]
```

#### Examples
{:.no_toc}

```
lakectl cat-hook-output lakefs://<repository> <run_id> <run_hook_id>
```

#### Options
{:.no_toc}

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
{:.no_toc}

```
      --amount int    how many records to return, or -1 for all records (default -1)
  -f, --file string   path to an sstable file, or "-" for stdin
  -h, --help          help for cat-sst
```



### lakectl commit

Commit changes on a given branch

```
lakectl commit <branch uri> [flags]
```

#### Options
{:.no_toc}

```
      --allow-empty-message   allow an empty commit message
  -h, --help                  help for commit
  -m, --message string        commit message
      --meta strings          key value pair in the form of key=value
```



### lakectl completion

Generate completion script

#### Synopsis
{:.no_toc}

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
{:.no_toc}

```
  -h, --help   help for completion
```



### lakectl config

Create/update local lakeFS configuration

```
lakectl config [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for config
```



### lakectl dbt

Integration with dbt commands

#### Options
{:.no_toc}

```
  -h, --help   help for dbt
```



### lakectl dbt create-branch-schema

Creates a new schema dedicated for branch and clones all dbt models to new schema

```
lakectl dbt create-branch-schema [flags]
```

#### Examples
{:.no_toc}

```
lakectl dbt create-branch-schema --branch <branch-name>
```

#### Options
{:.no_toc}

```
      --branch string               requested branch
      --continue-on-error           prevent command from failing when a single table fails
      --continue-on-schema-exists   allow running on existing schema
      --create-branch               create a new branch for the schema
      --dbfs-location string        
  -h, --help                        help for create-branch-schema
      --project-root string         location of dbt project (default ".")
      --skip-views                  
      --to-schema string            destination schema name [default is branch]
```



### lakectl dbt generate-schema-macro

generates the a macro allowing lakectl to run dbt on dynamic schemas

```
lakectl dbt generate-schema-macro [flags]
```

#### Examples
{:.no_toc}

```
lakectl dbt generate-schema-macro
```

#### Options
{:.no_toc}

```
  -h, --help                  help for generate-schema-macro
      --project-root string   location of dbt project (default ".")
```



### lakectl dbt help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type dbt help [path to command] for full details.

```
lakectl dbt help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl diff

Show changes between two commits, or the currently uncommitted changes

```
lakectl diff <ref uri> [ref uri] [flags]
```

#### Examples
{:.no_toc}

```

	lakectl diff lakefs://example-repo/example-branch
	Show uncommitted changes in example-branch.

	lakectl diff lakefs://example-repo/main lakefs://example-repo/dev
	This shows the differences between master and dev starting at the last common commit.
	This is similar to the three-dot (...) syntax in git.
	Uncommitted changes are not shown.

	lakectl diff --two-way lakefs://example-repo/main lakefs://example-repo/dev
	Show changes between the tips of the main and dev branches.
	This is similar to the two-dot (..) syntax in git.
	Uncommitted changes are not shown.

	lakectl diff --two-way lakefs://example-repo/main lakefs://example-repo/dev$
	Show changes between the tip of the main and the dev branch, including uncommitted changes on dev.
```

#### Options
{:.no_toc}

```
  -h, --help      help for diff
      --two-way   Use two-way diff: show difference between the given refs, regardless of a common ancestor.
```



### lakectl docs

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }



```
lakectl docs [outfile] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for docs
```



### lakectl doctor

Run a basic diagnosis of the LakeFS configuration

```
lakectl doctor [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for doctor
```



### lakectl fs

View and manipulate objects

#### Options
{:.no_toc}

```
  -h, --help   help for fs
```



### lakectl fs cat

Dump content of object to stdout

```
lakectl fs cat <path uri> [flags]
```

#### Options
{:.no_toc}

```
  -d, --direct   read directly from backing store (faster but requires more credentials)
  -h, --help     help for cat
```



### lakectl fs help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type fs help [path to command] for full details.

```
lakectl fs help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl fs ls

List entries under a given tree

```
lakectl fs ls <path uri> [flags]
```

#### Options
{:.no_toc}

```
  -h, --help        help for ls
      --recursive   list all objects under the specified prefix
```



### lakectl fs rm

Delete object

```
lakectl fs rm <path uri> [flags]
```

#### Options
{:.no_toc}

```
  -C, --concurrency int   max concurrent single delete operations to send to the lakeFS server (default 50)
  -h, --help              help for rm
  -r, --recursive         recursively delete all objects under the specified path
```



### lakectl fs stage

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

Stage a reference to an existing object, to be managed in lakeFS

```
lakectl fs stage <path uri> [flags]
```

#### Options
{:.no_toc}

```
      --checksum string       Object MD5 checksum as a hexadecimal string
      --content-type string   MIME type of contents
  -h, --help                  help for stage
      --location string       fully qualified storage location (i.e. "s3://bucket/path/to/object")
      --meta strings          key value pairs in the form of key=value
      --mtime int             Object modified time (Unix Epoch in seconds). Defaults to current time
      --size int              Object size in bytes
```



### lakectl fs stat

View object metadata

```
lakectl fs stat <path uri> [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for stat
```



### lakectl fs upload

Upload a local file to the specified URI

```
lakectl fs upload <path uri> [flags]
```

#### Options
{:.no_toc}

```
      --content-type string   MIME type of contents
  -d, --direct                write directly to backing store (faster but requires more credentials)
  -h, --help                  help for upload
  -r, --recursive             recursively copy all files under local source
  -s, --source string         local file to upload, or "-" for stdin
```



### lakectl gc

Manage garbage collection configuration

#### Options
{:.no_toc}

```
  -h, --help   help for gc
```



### lakectl gc get-config

Show garbage collection configuration JSON

```
lakectl gc get-config [flags]
```

#### Examples
{:.no_toc}

```
lakectl gc get-config <repository uri>
```

#### Options
{:.no_toc}

```
  -h, --help   help for get-config
  -p, --json   get rules as JSON
```



### lakectl gc help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type gc help [path to command] for full details.

```
lakectl gc help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl gc set-config

Set garbage collection configuration JSON

#### Synopsis
{:.no_toc}

Sets the garbage collection configuration JSON.
Example configuration file:
{
  "default_retention_days": 21,
  "branches": [
    {
      "branch_id": "main",
      "retention_days": 28
    },
    {
      "branch_id": "dev",
      "retention_days": 14
    }
  ]
}

```
lakectl gc set-config [flags]
```

#### Examples
{:.no_toc}

```
lakectl gc set-config <repository uri> -f config.json
```

#### Options
{:.no_toc}

```
  -f, --filename string   file containing the GC configuration
  -h, --help              help for set-config
```



### lakectl help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type lakectl help [path to command] for full details.

```
lakectl help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl ingest

Ingest objects from an external source into a lakeFS branch (without actually copying them)

```
lakectl ingest --from <object store URI> --to <lakeFS path URI> [--dry-run] [flags]
```

#### Options
{:.no_toc}

```
  -C, --concurrency int          max concurrent API calls to make to the lakeFS server (default 64)
      --dry-run                  only print the paths to be ingested
      --from string              prefix to read from (e.g. "s3://bucket/sub/path/"). must not be in a storage namespace
  -h, --help                     help for ingest
      --s3-endpoint-url string   URL to access S3 storage API (by default, use regular AWS S3 endpoint
      --to string                lakeFS path to load objects into (e.g. "lakefs://repo/branch/sub/path/")
```



### lakectl log

Show log of commits

#### Synopsis
{:.no_toc}

Show log of commits for a given branch

```
lakectl log <branch uri> [flags]
```

#### Options
{:.no_toc}

```
      --after string         show results after this value (used for pagination)
      --amount int           number of results to return. By default, all results are returned
  -h, --help                 help for log
      --limit                limit result just to amount. By default, returns whether more items are available.
      --objects strings      show results that contains changes to at least one path in that list of objects. Use comma separator to pass all objects together
      --prefixes strings     show results that contains changes to at least one path in that list of prefixes. Use comma separator to pass all prefixes together
      --show-meta-range-id   also show meta range ID
```



### lakectl merge

Merge & commit changes from source branch into destination branch

#### Synopsis
{:.no_toc}

Merge & commit changes from source branch into destination branch

```
lakectl merge <source ref> <destination ref> [flags]
```

#### Options
{:.no_toc}

```
  -h, --help              help for merge
      --strategy string   In case of a merge conflict, this option will force the merge process to automatically favor changes from the dest branch ("dest-wins") or from the source branch("source-wins"). In case no selection is made, the merge process will fail in case of a conflict
```



### lakectl metastore

Manage metastore commands

#### Options
{:.no_toc}

```
  -h, --help   help for metastore
```



### lakectl metastore copy

Copy or merge table

#### Synopsis
{:.no_toc}

Copy or merge table. the destination table will point to the selected branch

```
lakectl metastore copy [flags]
```

#### Options
{:.no_toc}

```
      --catalog-id string         Glue catalog ID
      --dbfs-root dbfs:/          dbfs location root will replace dbfs:/ in the location before transforming
      --from-client-type string   metastore type [hive, glue]
      --from-schema string        source schema name
      --from-table string         source table name
  -h, --help                      help for copy
      --metastore-uri string      Hive metastore URI
  -p, --partition strings         partition to copy
      --serde string              serde to set copy to  [default is  to-table]
      --to-branch string          lakeFS branch name
      --to-client-type string     metastore type [hive, glue]
      --to-schema string          destination schema name [default is from-branch]
      --to-table string           destination table name [default is  from-table] 
```



### lakectl metastore copy-all

Copy from one metastore to another

#### Synopsis
{:.no_toc}

copy or merge requested tables between hive metastores. the destination tables will point to the selected branch

```
lakectl metastore copy-all [flags]
```

#### Options
{:.no_toc}

```
      --branch string             lakeFS branch name
      --continue-on-error         prevent copy-all from failing when a single table fails
      --dbfs-root dbfs:/          dbfs location root will replace dbfs:/ in the location before transforming
      --from-address string       source metastore address
      --from-client-type string   metastore type [hive, glue]
  -h, --help                      help for copy-all
      --schema-filter string      filter for schemas to copy in metastore pattern (default ".*")
      --table-filter string       filter for tables to copy in metastore pattern (default ".*")
      --to-address string         destination metastore address
      --to-client-type string     metastore type [hive, glue]
```



### lakectl metastore copy-schema

Copy schema

#### Synopsis
{:.no_toc}

Copy schema (without tables). the destination schema will point to the selected branch

```
lakectl metastore copy-schema [flags]
```

#### Options
{:.no_toc}

```
      --catalog-id string         Glue catalog ID
      --dbfs-root dbfs:/          dbfs location root will replace dbfs:/ in the location before transforming
      --from-client-type string   metastore type [hive, glue]
      --from-schema string        source schema name
  -h, --help                      help for copy-schema
      --metastore-uri string      Hive metastore URI
      --to-branch string          lakeFS branch name
      --to-client-type string     metastore type [hive, glue]
      --to-schema string          destination schema name [default is from-branch]
```



### lakectl metastore create-symlink

Create symlink table and data

#### Synopsis
{:.no_toc}

create table with symlinks, and create the symlinks in s3 in order to access from external services that could only access s3 directly (e.g athena)

```
lakectl metastore create-symlink [flags]
```

#### Options
{:.no_toc}

```
      --branch string             lakeFS branch name
      --catalog-id string         Glue catalog ID
      --from-client-type string   metastore type [hive, glue]
      --from-schema string        source schema name
      --from-table string         source table name
  -h, --help                      help for create-symlink
      --path string               path to table on lakeFS
      --repo string               lakeFS repository name
      --to-schema string          destination schema name
      --to-table string           destination table name
```



### lakectl metastore diff

Show column and partition differences between two tables

```
lakectl metastore diff [flags]
```

#### Options
{:.no_toc}

```
      --catalog-id string         Glue catalog ID
      --from-address string       source metastore address
      --from-client-type string   metastore type [hive, glue]
      --from-schema string        source schema name
      --from-table string         source table name
  -h, --help                      help for diff
      --metastore-uri string      Hive metastore URI
      --to-address string         destination metastore address
      --to-client-type string     metastore type [hive, glue]
      --to-schema string          destination schema name 
      --to-table string           destination table name [default is from-table]
```



### lakectl metastore help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type metastore help [path to command] for full details.

```
lakectl metastore help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl metastore import-all

Import from one metastore to another

#### Synopsis
{:.no_toc}


import requested tables between hive metastores. the destination tables will point to the selected repository and branch
table with location s3://my-s3-bucket/path/to/table 
will be transformed to location s3://repo-param/bucket-param/path/to/table
	

```
lakectl metastore import-all [flags]
```

#### Options
{:.no_toc}

```
      --branch string             lakeFS branch name
      --continue-on-error         prevent import-all from failing when a single table fails
      --dbfs-root dbfs:/          dbfs location root will replace dbfs:/ in the location before transforming
      --from-address string       source metastore address
      --from-client-type string   metastore type [hive, glue]
  -h, --help                      help for import-all
      --repo string               lakeFS repo name
      --schema-filter string      filter for schemas to copy in metastore pattern (default ".*")
      --table-filter string       filter for tables to copy in metastore pattern (default ".*")
      --to-address string         destination metastore address
      --to-client-type string     metastore type [hive, glue]
```



### lakectl refs-dump

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

Dumps refs (branches, commits, tags) to the underlying object store

```
lakectl refs-dump <repository uri> [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for refs-dump
```



### lakectl refs-restore

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

Restores refs (branches, commits, tags) from the underlying object store to a bare repository

#### Synopsis
{:.no_toc}

restores refs (branches, commits, tags) from the underlying object store to a bare repository.

This command is expected to run on a bare repository (i.e. one created with 'lakectl repo create-bare').
Since a bare repo is expected, in case of transient failure, delete the repository and recreate it as bare and retry.

```
lakectl refs-restore <repository uri> [flags]
```

#### Examples
{:.no_toc}

```
aws s3 cp s3://bucket/_lakefs/refs_manifest.json - | lakectl refs-restore lakefs://my-bare-repository --manifest -
```

#### Options
{:.no_toc}

```
  -h, --help                 help for refs-restore
      --manifest refs-dump   path to a refs manifest json file (as generated by refs-dump). Alternatively, use "-" to read from stdin
```



### lakectl repo

Manage and explore repos

#### Options
{:.no_toc}

```
  -h, --help   help for repo
```



### lakectl repo create

Create a new repository

```
lakectl repo create <repository uri> <storage namespace> [flags]
```

#### Examples
{:.no_toc}

```
lakectl repo create lakefs://some-repo-name s3://some-bucket-name
```

#### Options
{:.no_toc}

```
  -d, --default-branch string   the default branch of this repository (default "main")
  -h, --help                    help for create
```



### lakectl repo create-bare

**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.
{: .note .note-warning }

Create a new repository with no initial branch or commit

```
lakectl repo create-bare <repository uri> <storage namespace> [flags]
```

#### Examples
{:.no_toc}

```
lakectl create-bare lakefs://some-repo-name s3://some-bucket-name
```

#### Options
{:.no_toc}

```
  -d, --default-branch string   the default branch name of this repository (will not be created) (default "main")
  -h, --help                    help for create-bare
```



### lakectl repo delete

Delete existing repository

```
lakectl repo delete <repository uri> [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for delete
  -y, --yes    Automatically say yes to all confirmations
```



### lakectl repo help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type repo help [path to command] for full details.

```
lakectl repo help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl repo list

List repositories

```
lakectl repo list [flags]
```

#### Options
{:.no_toc}

```
      --after string   show results after this value (used for pagination)
      --amount int     number of results to return (default 100)
  -h, --help           help for list
```



### lakectl show

See detailed information about an entity by ID (commit, user, etc)

```
lakectl show <repository uri> [flags]
```

#### Options
{:.no_toc}

```
      --commit string        commit ID to show
  -h, --help                 help for show
      --show-meta-range-id   when showing commits, also show meta range ID
```



### lakectl tag

Create and manage tags within a repository

#### Synopsis
{:.no_toc}

Create delete and list tags within a lakeFS repository

#### Options
{:.no_toc}

```
  -h, --help   help for tag
```



### lakectl tag create

Create a new tag in a repository

```
lakectl tag create <tag uri> <commit uri> [flags]
```

#### Examples
{:.no_toc}

```
lakectl tag create lakefs://example-repo/example-tag lakefs://example-repo/2397cc9a9d04c20a4e5739b42c1dd3d8ba655c0b3a3b974850895a13d8bf9917
```

#### Options
{:.no_toc}

```
  -f, --force   override the tag if it exists
  -h, --help    help for create
```



### lakectl tag delete

Delete a tag from a repository

```
lakectl tag delete <tag uri> [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for delete
```



### lakectl tag help

Help about any command

#### Synopsis
{:.no_toc}

Help provides help for any command in the application.
Simply type tag help [path to command] for full details.

```
lakectl tag help [command] [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for help
```



### lakectl tag list

List tags in a repository

```
lakectl tag list <repository uri> [flags]
```

#### Examples
{:.no_toc}

```
lakectl tag list lakefs://<repository>
```

#### Options
{:.no_toc}

```
      --after string   show results after this value (used for pagination)
      --amount int     number of results to return (default 100)
  -h, --help           help for list
```



### lakectl tag show

Show tag's commit reference

```
lakectl tag show <tag uri> [flags]
```

#### Options
{:.no_toc}

```
  -h, --help   help for show
```



