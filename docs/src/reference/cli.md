---
title: lakectl (lakeFS command-line tool)
description: lakeFS comes with its own native CLI client. Here you can see the complete command reference.
---

# lakectl (lakeFS command-line tool)

!!! note
	This file (cli.md) is automatically generated from the Go code files under `cmd/lakectl`. 
	Any changes made directly to the Markdown file will be overwritten, and should instead be made to the
	relevant Go files. 

## Installing lakectl locally

`lakectl` is available for Linux, macOS, and Windows. You can also [run it using Docker](#running-lakectl-from-docker).

[:material-download-outline: Download lakectl](https://github.com/treeverse/lakeFS/releases){: .md-button .md-button--primary target="_blank"}

Or using [Homebrew](https://brew.sh/) for Linux/macOS:

```bash
brew tap treeverse/lakefs
brew install lakefs
```

### Configuring credentials and API endpoint

Once you've installed the lakectl command, run:

```bash
lakectl config
# output:
# Config file /home/janedoe/.lakectl.yaml will be used
# Access key ID: AKIAIOSFODNN7EXAMPLE
# Secret access key: ****************************************
# Server endpoint URL: http://localhost:8000
```

This will setup a `$HOME/.lakectl.yaml` file with the credentials and API endpoint you've supplied.
When setting up a new installation and creating initial credentials (see [Quickstart](../quickstart/index.md)), the UI
will provide a link to download a preconfigured configuration file for you.

## lakectl Configuration

`lakectl` reads its configuration from a YAML file (default path `~/.lakectl.yaml`, overridable with `--config` or `LAKECTL_CONFIG_FILE`) and/or from environment variables.

* Every configuration key can be supplied through an environment variable using the pattern `LAKECTL_<UPPERCASE_KEY_WITH_DOTS_REPLACED_BY_UNDERSCORES>`.
* Any value given on the command-line flags overrides the value in the configuration file, which in turn overrides the value supplied through the environment.

### Reference

* `credentials.access_key_id` `(string : required)` - Access-key ID used to authenticate against lakeFS.
* `credentials.secret_access_key` `(string : required)`  - Secret access key paired with the access key ID.
* `credentials.provider.type` `(string : "")` - Enterprise only. Set to `aws_iam` to obtain temporary credentials from AWS IAM; empty for static credentials (default).
  * `credentials.provider.aws_iam.token_ttl_seconds` `(duration : 6h)` - Lifetime of the generated lakeFS token.
  * `credentials.provider.aws_iam.url_presign_ttl_seconds` `(duration : 1m)` - TTL of pre-signed URLs created by lakectl.
  * `credentials.provider.aws_iam.refresh_interval` `(duration : 5m)` - How often lakectl refreshes the IAM credentials.
  * `credentials.provider.aws_iam.token_request_headers` `(map[string]string : {})` - Extra HTTP headers to include when requesting the token.
* `network.http2.enabled` `(bool : true)` - Enable HTTP/2 for the API client.
* `server.endpoint_url` `(string : ` http://127.0.0.1:8000 `) - Base URL of the lakeFS server.
* `server.retries.enabled` `(bool : true)` - Whether lakectl tries more than once.
* `server.retries.max_attempts` `(uint : 4)` - Maximum number of attempts per request.
* `server.retries.min_wait_interval` `(duration : 200ms)` - Minimum back-off between retries.
* `server.retries.max_wait_interval` `(duration : 30s)` - Maximum back-off between retries.
* `options.parallelism` `(int : 25)` - Default concurrency level for I/O operations (upload, download, etc.).
* `local.skip_non_regular_files` `(bool : false)` - When true, symbolic links and other non-regular files are skipped during `lakectl local` operations instead of causing an error.
* `experimental.local.posix_permissions.enabled` `(bool : false)` - Preserve POSIX permissions when syncing files.
  * `experimental.local.posix_permissions.include_uid` `(bool : false)` - Include UID in the stored metadata.
  * `experimental.local.posix_permissions.include_gid` `(bool : false)` - Include GID in the stored metadata.


## Running lakectl from Docker

If you'd rather run `lakectl` from a Docker container you can do so by passing configuration elements as environment variables. 
Here is an example: 

```bash
docker run --rm --pull always \
          -e LAKECTL_CREDENTIALS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
          -e LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=xxxxx
          -e LAKECTL_SERVER_ENDPOINT_URL=https://host.us-east-2.lakefscloud.io/ \
          --entrypoint lakectl treeverse/lakefs \
          repo list
```

_Bear in mind that if you are running lakeFS itself locally you will need to account for this in your networking configuration of 
the Docker container. That is to say, `localhost` to a Docker container is itself, not the host machine on which it is running._

## Command Reference

### lakectl

A cli tool to explore manage and work with lakeFS

<h4>Synopsis</h4>

lakectl is a CLI tool allowing exploration and manipulation of a lakeFS environment.

It can be extended with plugins; see 'lakectl plugin --help' for more information.

```
lakectl [flags]
```

<h4>Options</h4>

```
      --base-uri string      base URI used for lakeFS address parse
  -c, --config string        config file (default is $HOME/.lakectl.yaml)
  -h, --help                 help for lakectl
      --log-format string    set logging output format
      --log-level string     set logging level (default "none")
      --log-output strings   set logging output(s)
      --no-color             don't use fancy output colors (default value can be set by NO_COLOR environment variable)
      --verbose              run in verbose mode
  -v, --version              version for lakectl
```

!!! note
    The `base-uri` option can be controlled with the `LAKECTL_BASE_URI` environment variable.

<h4>Example usage</h4>

```shell
$ export LAKECTL_BASE_URI="lakefs://my-repo/my-branch"
# Once set, use relative lakefs uri's:
$ lakectl fs ls /path
```

### lakectl actions

Manage Actions commands

<h4>Options</h4>

```
  -h, --help   help for actions
```



### lakectl actions help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type actions help [path to command] for full details.

```
lakectl actions help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl actions runs

Explore runs information

<h4>Options</h4>

```
  -h, --help   help for runs
```



### lakectl actions runs describe

Describe run results

<h4>Synopsis</h4>

Show information about the run and all the hooks that were executed as part of the run

```
lakectl actions runs describe <repository URI> <run_id> [flags]
```

<h4>Examples</h4>

```
lakectl actions runs describe lakefs://my-repo 20230719152411arS0z6I
```

<h4>Options</h4>

```
      --after string   show results after this value (used for pagination)
      --amount int     number of results to return. By default, all results are returned.
  -h, --help           help for describe
```



### lakectl actions runs help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type runs help [path to command] for full details.

```
lakectl actions runs help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl actions runs list

List runs

<h4>Synopsis</h4>

List all runs on a repository optional filter by branch or commit

```
lakectl actions runs list <repository URI> [--branch <branch>] [--commit <commit_id>] [flags]
```

<h4>Examples</h4>

```
lakectl actions runs list lakefs://my-repo --branch my-branch --commit 600dc0ffee
```

<h4>Options</h4>

```
      --branch string   show results for specific branch
      --commit string   show results for specific commit ID
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
  -h, --help            help for list
```



### lakectl actions validate

Validate action file

<h4>Synopsis</h4>

Tries to parse the input action file as lakeFS action file

```
lakectl actions validate [flags]
```

<h4>Examples</h4>

```
lakectl actions validate path/to/my/file
```

<h4>Options</h4>

```
  -h, --help   help for validate
```



### lakectl annotate

List entries under a given path, annotating each with the latest modifying commit

```
lakectl annotate <path URI> [flags]
```

<h4>Options</h4>

```
      --first-parent   follow only the first parent commit upon seeing a merge commit
  -h, --help           help for annotate
  -r, --recursive      recursively annotate all entries under a given path or prefix
```



### lakectl auth

Manage authentication and authorization

<h4>Synopsis</h4>

Manage authentication and authorization including users, groups and ACLs
This functionality is supported with an external auth service only.

<h4>Options</h4>

```
  -h, --help   help for auth
```



### lakectl auth groups

Manage groups

<h4>Options</h4>

```
  -h, --help   help for groups
```



### lakectl auth groups acl

Manage ACLs

<h4>Synopsis</h4>

manage ACLs of groups

<h4>Options</h4>

```
  -h, --help   help for acl
```



### lakectl auth groups acl get

Get ACL of group

```
lakectl auth groups acl get [flags]
```

<h4>Options</h4>

```
  -h, --help        help for get
      --id string   Group identifier
```



### lakectl auth groups acl help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type acl help [path to command] for full details.

```
lakectl auth groups acl help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth groups acl set

Set ACL of group

<h4>Synopsis</h4>

Set ACL of group. permission will be attached to all repositories.

```
lakectl auth groups acl set [flags]
```

<h4>Options</h4>

```
  -h, --help                help for set
      --id string           Group identifier
      --permission string   Permission, typically one of "Read", "Write", "Super" or "Admin"
```



### lakectl auth groups create

Create a group

```
lakectl auth groups create [flags]
```

<h4>Options</h4>

```
  -h, --help        help for create
      --id string   Group identifier
```



### lakectl auth groups delete

Delete a group

```
lakectl auth groups delete [flags]
```

<h4>Options</h4>

```
  -h, --help        help for delete
      --id string   Group identifier
```



### lakectl auth groups help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type groups help [path to command] for full details.

```
lakectl auth groups help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth groups list

List groups

```
lakectl auth groups list [flags]
```

<h4>Options</h4>

```
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl auth groups members

Manage group user memberships

<h4>Options</h4>

```
  -h, --help   help for members
```



### lakectl auth groups members add

Add a user to a group

```
lakectl auth groups members add [flags]
```

<h4>Options</h4>

```
  -h, --help          help for add
      --id string     Group identifier
      --user string   Username (email for password-based users, default: current user)
```



### lakectl auth groups members help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type members help [path to command] for full details.

```
lakectl auth groups members help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth groups members list

List users in a group

```
lakectl auth groups members list [flags]
```

<h4>Options</h4>

```
      --id string       Group identifier
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl auth groups members remove

Remove a user from a group

```
lakectl auth groups members remove [flags]
```

<h4>Options</h4>

```
  -h, --help          help for remove
      --id string     Group identifier
      --user string   Username (email for password-based users, default: current user)
```



### lakectl auth groups policies

Manage group policies

<h4>Synopsis</h4>

Manage group policies.  Requires an external authorization server with matching support.

<h4>Options</h4>

```
  -h, --help   help for policies
```



### lakectl auth groups policies attach

Attach a policy to a group

```
lakectl auth groups policies attach [flags]
```

<h4>Options</h4>

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

<h4>Options</h4>

```
  -h, --help            help for detach
      --id string       User identifier
      --policy string   Policy identifier
```



### lakectl auth groups policies help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth groups policies help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth groups policies list

List policies for the given group

```
lakectl auth groups policies list [flags]
```

<h4>Options</h4>

```
      --id string       Group identifier
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl auth help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type auth help [path to command] for full details.

```
lakectl auth help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth policies

Manage policies

<h4>Options</h4>

```
  -h, --help   help for policies
```



### lakectl auth policies create

Create a policy

```
lakectl auth policies create [flags]
```

<h4>Options</h4>

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

<h4>Options</h4>

```
  -h, --help        help for delete
      --id string   Policy identifier
```



### lakectl auth policies help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth policies help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth policies list

List policies

```
lakectl auth policies list [flags]
```

<h4>Options</h4>

```
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl auth policies show

Show a policy

```
lakectl auth policies show [flags]
```

<h4>Options</h4>

```
  -h, --help        help for show
      --id string   Policy identifier
```



### lakectl auth users

Manage users

<h4>Options</h4>

```
  -h, --help   help for users
```



### lakectl auth users create

Create a user

```
lakectl auth users create [flags]
```

<h4>Options</h4>

```
  -h, --help        help for create
      --id string   Username
```



### lakectl auth users credentials

Manage user credentials

<h4>Options</h4>

```
  -h, --help   help for credentials
```



### lakectl auth users credentials create

Create user credentials

```
lakectl auth users credentials create [flags]
```

<h4>Options</h4>

```
  -h, --help        help for create
      --id string   Username (email for password-based users, default: current user)
```



### lakectl auth users credentials delete

Delete user credentials

```
lakectl auth users credentials delete [flags]
```

<h4>Options</h4>

```
      --access-key-id string   Access key ID to delete
  -h, --help                   help for delete
      --id string              Username (email for password-based users, default: current user)
```



### lakectl auth users credentials help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type credentials help [path to command] for full details.

```
lakectl auth users credentials help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth users credentials list

List user credentials

```
lakectl auth users credentials list [flags]
```

<h4>Options</h4>

```
      --id string       Username (email for password-based users, default: current user)
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl auth users delete

Delete a user

```
lakectl auth users delete [flags]
```

<h4>Options</h4>

```
  -h, --help        help for delete
      --id string   Username (email for password-based users)
```



### lakectl auth users groups

Manage user groups

<h4>Options</h4>

```
  -h, --help   help for groups
```



### lakectl auth users groups help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type groups help [path to command] for full details.

```
lakectl auth users groups help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth users groups list

List groups for the given user

```
lakectl auth users groups list [flags]
```

<h4>Options</h4>

```
      --id string       Username (email for password-based users)
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl auth users help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type users help [path to command] for full details.

```
lakectl auth users help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth users list

List users

```
lakectl auth users list [flags]
```

<h4>Options</h4>

```
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl auth users policies

Manage user policies

<h4>Synopsis</h4>

Manage user policies.  Requires an external authorization server with matching support.

<h4>Options</h4>

```
  -h, --help   help for policies
```



### lakectl auth users policies attach

Attach a policy to a user

```
lakectl auth users policies attach [flags]
```

<h4>Options</h4>

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

<h4>Options</h4>

```
  -h, --help            help for detach
      --id string       Username (email for password-based users)
      --policy string   Policy identifier
```



### lakectl auth users policies help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type policies help [path to command] for full details.

```
lakectl auth users policies help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl auth users policies list

List policies for the given user

```
lakectl auth users policies list [flags]
```

<h4>Options</h4>

```
      --effective       List all distinct policies attached to the user, including by group memberships
      --id string       Username (email for password-based users)
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl branch

Create and manage branches within a repository

<h4>Synopsis</h4>

Create delete and list branches within a lakeFS repository

<h4>Options</h4>

```
  -h, --help   help for branch
```



### lakectl branch create

Create a new branch in a repository

```
lakectl branch create <branch URI> -s <source ref URI> [flags]
```

<h4>Examples</h4>

```
lakectl branch create lakefs://example-repo/new-branch -s lakefs://example-repo/main
```

<h4>Options</h4>

```
  -h, --help            help for create
  -s, --source string   source branch uri
```



### lakectl branch delete

Delete a branch in a repository, along with its uncommitted changes (CAREFUL)

```
lakectl branch delete <branch URI> [flags]
```

<h4>Examples</h4>

```
lakectl branch delete lakefs://my-repo/my-branch
```

<h4>Options</h4>

```
  -h, --help   help for delete
  -y, --yes    Automatically say yes to all confirmations
```



### lakectl branch help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type branch help [path to command] for full details.

```
lakectl branch help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl branch list

List branches in a repository

```
lakectl branch list <repository URI> [flags]
```

<h4>Examples</h4>

```
lakectl branch list lakefs://my-repo
```

<h4>Options</h4>

```
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl branch reset

Reset uncommitted changes - all of them, or by path

<h4>Synopsis</h4>

reset changes.  There are four different ways to reset changes:
  1. reset all uncommitted changes - reset lakefs://myrepo/main 
  2. reset uncommitted changes under specific path - reset lakefs://myrepo/main --prefix path
  3. reset uncommitted changes for specific object - reset lakefs://myrepo/main --object path

```
lakectl branch reset <branch URI> [--prefix|--object] [flags]
```

<h4>Examples</h4>

```
lakectl branch reset lakefs://my-repo/my-branch
```

<h4>Options</h4>

```
  -h, --help            help for reset
      --object string   path to object to be reset
      --prefix string   prefix of the objects to be reset
  -y, --yes             Automatically say yes to all confirmations
```



### lakectl branch revert

Given a commit, record a new commit to reverse the effect of this commit

<h4>Synopsis</h4>

The commits will be reverted in left-to-right order

```
lakectl branch revert <branch URI> <commit ref to revert> [<more commits>...] [flags]
```

<h4>Examples</h4>

```
lakectl branch revert lakefs://example-repo/example-branch commitA
	          Revert the changes done by commitA in example-branch
		      branch revert lakefs://example-repo/example-branch HEAD~1 HEAD~2 HEAD~3
		      Revert the changes done by the second last commit to the fourth last commit in example-branch
```

<h4>Options</h4>

```
      --allow-empty-commit   allow empty commit (revert without changes)
  -h, --help                 help for revert
  -m, --parent-number int    the parent number (starting from 1) of the mainline. The revert will reverse the change relative to the specified parent.
  -y, --yes                  Automatically say yes to all confirmations
```



### lakectl branch show

Show branch latest commit reference

```
lakectl branch show <branch URI> [flags]
```

<h4>Examples</h4>

```
lakectl branch show lakefs://my-repo/my-branch
```

<h4>Options</h4>

```
  -h, --help   help for show
```



### lakectl branch-protect

Create and manage branch protection rules

<h4>Synopsis</h4>

Define branch protection rules to prevent direct changes. Changes to protected branches can only be done by merging from other branches.

<h4>Options</h4>

```
  -h, --help   help for branch-protect
```



### lakectl branch-protect add

Add a branch protection rule

<h4>Synopsis</h4>

Add a branch protection rule for a given branch name pattern

```
lakectl branch-protect add <repository URI> <pattern> [flags]
```

<h4>Examples</h4>

```
lakectl branch-protect add lakefs://my-repo 'stable_*'
```

<h4>Options</h4>

```
  -h, --help   help for add
```



### lakectl branch-protect delete

Delete a branch protection rule

<h4>Synopsis</h4>

Delete a branch protection rule for a given branch name pattern

```
lakectl branch-protect delete <repository URI> <pattern> [flags]
```

<h4>Examples</h4>

```
lakectl branch-protect delete lakefs://my-repo stable_*
```

<h4>Options</h4>

```
  -h, --help   help for delete
```



### lakectl branch-protect help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type branch-protect help [path to command] for full details.

```
lakectl branch-protect help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl branch-protect list

List all branch protection rules

```
lakectl branch-protect list <repository URI> [flags]
```

<h4>Examples</h4>

```
lakectl branch-protect list lakefs://my-repo
```

<h4>Options</h4>

```
  -h, --help   help for list
```



### lakectl cherry-pick

Apply the changes introduced by an existing commit

<h4>Synopsis</h4>

Apply the changes from the given commit to the tip of the branch. The changes will be added as a new commit.

```
lakectl cherry-pick <commit URI> <branch> [flags]
```

<h4>Examples</h4>

```
lakectl cherry-pick lakefs://my-repo/600dc0ffee lakefs://my-repo/my-branch
```

<h4>Options</h4>

```
  -h, --help                help for cherry-pick
  -m, --parent-number int   the parent number (starting from 1) of the cherry-picked commit. The cherry-pick will apply the change relative to the specified parent.
```



### lakectl commit

Commit changes on a given branch

```
lakectl commit <branch URI> [flags]
```

<h4>Options</h4>

```
      --allow-empty-commit    allow a commit with no changes
      --allow-empty-message   allow an empty commit message
  -h, --help                  help for commit
  -m, --message string        commit message
      --meta strings          key value pair in the form of key=value
```



### lakectl completion

Generate completion script

<h4>Synopsis</h4>

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

<h4>Options</h4>

```
  -h, --help   help for completion
```



### lakectl config

Create/update local lakeFS configuration

```
lakectl config [flags]
```

<h4>Options</h4>

```
  -h, --help   help for config
```



### lakectl diff

Show changes between two commits, or the currently uncommitted changes

```
lakectl diff <ref URI> [ref URI] [flags]
```

<h4>Examples</h4>

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
	
	lakectl diff --prefix some/path lakefs://example-repo/main lakefs://example-repo/dev
	Show changes of objects prefixed with 'some/path' between the tips of the main and dev branches.
```

<h4>Options</h4>

```
  -h, --help            help for diff
      --prefix string   Show only changes in the given prefix.
      --two-way         Use two-way diff: show difference between the given refs, regardless of a common ancestor.
```



### lakectl doctor

Run a basic diagnosis of the LakeFS configuration

```
lakectl doctor [flags]
```

<h4>Options</h4>

```
  -h, --help   help for doctor
```



### lakectl fs

View and manipulate objects

<h4>Options</h4>

```
  -h, --help   help for fs
```



### lakectl fs cat

Dump content of object to stdout

```
lakectl fs cat <path URI> [flags]
```

<h4>Options</h4>

```
  -h, --help       help for cat
      --pre-sign   Use pre-signed URLs when downloading/uploading data (recommended) (default true)
```



### lakectl fs download

Download object(s) from a given repository path

```
lakectl fs download <path URI> [<destination path>] [flags]
```

<h4>Options</h4>

```
  -h, --help              help for download
      --no-progress       Disable progress bar animation for IO operations
  -p, --parallelism int   Max concurrent operations to perform (default 25)
      --part-size int     part size in bytes for multipart download (default 8388608)
      --pre-sign          Use pre-signed URLs when downloading/uploading data (recommended) (default true)
  -r, --recursive         recursively download all objects under path
```



### lakectl fs help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type fs help [path to command] for full details.

```
lakectl fs help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl fs ls

List entries under a given tree

```
lakectl fs ls <path URI> [flags]
```

<h4>Options</h4>

```
  -h, --help        help for ls
  -r, --recursive   list all objects under the specified path
```



### lakectl fs presign

return a pre-signed URL for reading the specified object

```
lakectl fs presign <path URI> [flags]
```

<h4>Options</h4>

```
  -h, --help   help for presign
```



### lakectl fs rm

Delete object

```
lakectl fs rm <path URI> [flags]
```

<h4>Options</h4>

```
  -C, --concurrency int   max concurrent single delete operations to send to the lakeFS server (default 50)
  -h, --help              help for rm
  -r, --recursive         recursively delete all objects under the specified path
```



### lakectl fs stat

View object metadata

```
lakectl fs stat <path URI> [flags]
```

<h4>Options</h4>

```
  -h, --help       help for stat
      --pre-sign   Use pre-signed URLs when downloading/uploading data (recommended) (default true)
```



### lakectl fs upload

Upload a local file to the specified URI

```
lakectl fs upload <path URI> [flags]
```

<h4>Options</h4>

```
      --content-type string   MIME type of contents
  -h, --help                  help for upload
      --no-progress           Disable progress bar animation for IO operations
  -p, --parallelism int       Max concurrent operations to perform (default 25)
      --pre-sign              Use pre-signed URLs when downloading/uploading data (recommended) (default true)
  -r, --recursive             recursively copy all files under local source
  -s, --source string         local file to upload, or "-" for stdin
```




---------
### lakectl fs stage

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Link an external object with a path in a repository

<h4>Synopsis</h4>

Link an external object with a path in a repository, creating an uncommitted change.
The object location must be outside the repository's storage namespace

```
lakectl fs stage <path URI> [flags]
```

<h4>Options</h4>

```
      --checksum string       Object MD5 checksum as a hexadecimal string
      --content-type string   MIME type of contents
  -h, --help                  help for stage
      --location string       fully qualified storage location (i.e. "s3://bucket/path/to/object")
      --meta strings          key value pairs in the form of key=value
      --mtime int             Object modified time (Unix Epoch in seconds). Defaults to current time
      --size int              Object size in bytes
```



### lakectl fs update-metadata

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Update user metadata on the specified URI

```
lakectl fs update-metadata <path URI> [flags]
```

<h4>Options</h4>

```
  -h, --help               help for update-metadata
      --metadata strings   Metadata to set, in the form key1=value1,key2=value2
```



### lakectl gc

Manage the garbage collection policy

<h4>Options</h4>

```
  -h, --help   help for gc
```



### lakectl gc delete-config

Deletes the garbage collection policy for the repository

```
lakectl gc delete-config <repository URI> [flags]
```

<h4>Examples</h4>

```
lakectl gc delete-config lakefs://my-repo
```

<h4>Options</h4>

```
  -h, --help   help for delete-config
```



### lakectl gc get-config

Show the garbage collection policy for this repository

```
lakectl gc get-config <repository URI> [flags]
```

<h4>Examples</h4>

```
lakectl gc get-config lakefs://my-repo
```

<h4>Options</h4>

```
  -h, --help   help for get-config
  -p, --json   get rules as JSON
```



### lakectl gc help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type gc help [path to command] for full details.

```
lakectl gc help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl gc set-config

Set garbage collection policy JSON

<h4>Synopsis</h4>

Sets the garbage collection policy JSON.
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
lakectl gc set-config <repository URI> [flags]
```

<h4>Examples</h4>

```
lakectl gc set-config lakefs://my-repo -f config.json
```

<h4>Options</h4>

```
  -f, --filename string   file containing the GC policy as JSON
  -h, --help              help for set-config
```



### lakectl help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type lakectl help [path to command] for full details.

```
lakectl help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl identity

Show identity info

<h4>Synopsis</h4>

Show the info of the configured user in lakectl

```
lakectl identity [flags]
```

<h4>Examples</h4>

```
lakectl identity
```

<h4>Options</h4>

```
  -h, --help   help for identity
```



### lakectl import

Import data from external source to a destination branch

```
lakectl import --from <object store URI> --to <lakeFS path URI> [flags]
```

<h4>Options</h4>

```
      --allow-empty-message   allow an empty commit message (default true)
      --from string           prefix to read from (e.g. "s3://bucket/sub/path/"). must not be in a storage namespace
  -h, --help                  help for import
  -m, --message string        commit message
      --meta strings          key value pair in the form of key=value
      --no-progress           switch off the progress output
      --to string             lakeFS path to load objects into (e.g. "lakefs://repo/branch/sub/path/")
```



### lakectl local

Sync local directories with lakeFS paths

<h4>Options</h4>

```
  -h, --help   help for local
```



### lakectl local checkout

Sync local directory with the remote state.

```
lakectl local checkout [directory] [flags]
```

<h4>Options</h4>

```
      --all               Checkout given source branch or reference for all linked directories
  -h, --help              help for checkout
      --no-progress       Disable progress bar animation for IO operations
  -p, --parallelism int   Max concurrent operations to perform (default 25)
      --pre-sign          Use pre-signed URLs when downloading/uploading data (recommended) (default true)
  -r, --ref string        Checkout the given reference
  -y, --yes               Automatically say yes to all confirmations
```



### lakectl local clone

Clone a path from a lakeFS repository into a new directory.

```
lakectl local clone <path URI> [directory] [flags]
```

<h4>Options</h4>

```
      --gitignore         Update .gitignore file when working in a git repository context (default true)
  -h, --help              help for clone
      --no-progress       Disable progress bar animation for IO operations
  -p, --parallelism int   Max concurrent operations to perform (default 25)
      --pre-sign          Use pre-signed URLs when downloading/uploading data (recommended) (default true)
```



### lakectl local commit

Commit changes from local directory to the lakeFS branch it tracks.

```
lakectl local commit [directory] [flags]
```

<h4>Options</h4>

```
      --allow-empty-message   allow an empty commit message
      --force                 Commit changes even if remote branch includes uncommitted changes external to the synced path
  -h, --help                  help for commit
  -m, --message string        commit message
      --meta strings          key value pair in the form of key=value
      --no-progress           Disable progress bar animation for IO operations
  -p, --parallelism int       Max concurrent operations to perform (default 25)
      --pre-sign              Use pre-signed URLs when downloading/uploading data (recommended) (default true)
```



### lakectl local help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type local help [path to command] for full details.

```
lakectl local help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl local init

set a local directory to sync with a lakeFS path.

```
lakectl local init <path URI> [directory] [flags]
```

<h4>Options</h4>

```
      --force       Overwrites if directory already linked to a lakeFS path
      --gitignore   Update .gitignore file when working in a git repository context (default true)
  -h, --help        help for init
```



### lakectl local list

find and list directories that are synced with lakeFS.

```
lakectl local list [directory] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for list
```



### lakectl local pull

Fetch latest changes from lakeFS.

```
lakectl local pull [directory] [flags]
```

<h4>Options</h4>

```
      --force             Reset any uncommitted local change
  -h, --help              help for pull
      --no-progress       Disable progress bar animation for IO operations
  -p, --parallelism int   Max concurrent operations to perform (default 25)
      --pre-sign          Use pre-signed URLs when downloading/uploading data (recommended) (default true)
```



### lakectl local status

show modifications (both remote and local) to the directory and the remote location it tracks

```
lakectl local status [directory] [flags]
```

<h4>Options</h4>

```
  -h, --help    help for status
  -l, --local   Don't compare against remote changes
```



### lakectl log

Show log of commits

<h4>Synopsis</h4>

Show log of commits for a given reference

```
lakectl log <ref URI> [flags]
```

<h4>Examples</h4>

```
lakectl log --dot lakefs://example-repository/main | dot -Tsvg > graph.svg
```

<h4>Options</h4>

```
      --after string         show results after this value (used for pagination)
      --amount int           number of results to return. By default, all results are returned
      --dot                  return results in a dotgraph format
      --first-parent         follow only the first parent commit upon seeing a merge commit
  -h, --help                 help for log
      --limit                limit result just to amount. By default, returns whether more items are available.
      --no-merges            skip merge commits
      --objects strings      show results that contains changes to at least one path in that list of objects. Use comma separator to pass all objects together
      --prefixes strings     show results that contains changes to at least one path in that list of prefixes. Use comma separator to pass all prefixes together
      --show-meta-range-id   also show meta range ID
      --since string         show results since this date-time (RFC3339 format)
      --stop-at string       a Ref to stop at (included in results)
```



### lakectl merge

Merge & commit changes from source branch into destination branch

<h4>Synopsis</h4>

Merge & commit changes from source branch into destination branch

```
lakectl merge <source ref> <destination ref> [flags]
```

<h4>Options</h4>

```
      --allow-empty           Allow merge when the branches have the same content
      --allow-empty-message   allow an empty commit message (default true)
      --force                 Allow merge into a read-only branch or into a branch with the same content
  -h, --help                  help for merge
  -m, --message string        commit message
      --meta strings          key value pair in the form of key=value
      --squash                Squash all changes from source into a single commit on destination
      --strategy string       In case of a merge conflict, this option will force the merge process to automatically favor changes from the dest branch ("dest-wins") or from the source branch("source-wins"). In case no selection is made, the merge process will fail in case of a conflict
```



### lakectl plugin

Manage lakectl plugins

<h4>Synopsis</h4>

Provides utilities for managing lakectl plugins.

Plugins are standalone executable files that extend lakectl's functionality.
lakectl discovers plugins by looking for executables in your PATH
that are named with the prefix "lakectl-".

For example, an executable named "lakectl-myfeature" can be invoked as
"lakectl myfeature [args...]".

Plugin Naming:
  - The executable must start with "lakectl-".
  - The part after "lakectl-" becomes the command name users type.
    (e.g., "lakectl-foo" -> "lakectl foo")
  - The plugin name is used exactly as-is in the command.
    (e.g., "lakectl-foo-bar" -> "lakectl foo-bar")

Installation:
  - Place your "lakectl-..." executable file (which may be any executable,
    e.g. a Python application) in a directory listed in your PATH.
  - Ensure the file has execute permissions.

Execution:
  - When you run "lakectl some-plugin arg1 --flag", lakectl searches for
    "lakectl-some-plugin" in PATH.
  - If found and executable, it runs the plugin, passing "arg1 --flag" as arguments.
  - The plugin inherits environment variables from lakectl.
  - Standard output, standard error, and the exit code of the plugin are propagated.
  - Built-in lakectl commands always take precedence over plugins.

Use "lakectl plugin list" to see all detected plugins and any warnings.


<h4>Options</h4>

```
  -h, --help   help for plugin
```



### lakectl plugin help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type plugin help [path to command] for full details.

```
lakectl plugin help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl plugin list

List available lakectl plugins

<h4>Synopsis</h4>

Scans the PATH for executables named "lakectl-*" and lists the detected plugins.

```
lakectl plugin list [flags]
```

<h4>Options</h4>

```
  -h, --help   help for list
```



### lakectl repo

Manage and explore repos

<h4>Options</h4>

```
  -h, --help   help for repo
```



### lakectl repo create

Create a new repository

```
lakectl repo create <repository URI> <storage namespace> [flags]
```

<h4>Examples</h4>

```
lakectl repo create lakefs://my-repo s3://my-bucket
```

<h4>Options</h4>

```
  -d, --default-branch string   the default branch of this repository (default "main")
  -h, --help                    help for create
      --sample-data             create sample data in the repository
```



### lakectl repo delete

Delete existing repository

```
lakectl repo delete <repository URI> [flags]
```

<h4>Examples</h4>

```
lakectl repo delete lakefs://my-repo
```

<h4>Options</h4>

```
  -h, --help   help for delete
  -y, --yes    Automatically say yes to all confirmations
```



### lakectl repo help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type repo help [path to command] for full details.

```
lakectl repo help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl repo list

List repositories

```
lakectl repo list [flags]
```

<h4>Options</h4>

```
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```




---------
### lakectl repo create-bare

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Create a new repository with no initial branch or commit

```
lakectl repo create-bare <repository URI> <storage namespace> [flags]
```

<h4>Examples</h4>

```
lakectl create-bare lakefs://my-repo s3://my-bucket
```

<h4>Options</h4>

```
  -d, --default-branch string   the default branch name of this repository (will not be created) (default "main")
  -h, --help                    help for create-bare
```



### lakectl show

See detailed information about an entity

<h4>Options</h4>

```
  -h, --help   help for show
```



### lakectl show commit

See detailed information about a commit

```
lakectl show commit <commit URI> [flags]
```

<h4>Options</h4>

```
  -h, --help                 help for commit
      --show-meta-range-id   show meta range ID
```



### lakectl show help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type show help [path to command] for full details.

```
lakectl show help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl tag

Create and manage tags within a repository

<h4>Synopsis</h4>

Create delete and list tags within a lakeFS repository

<h4>Options</h4>

```
  -h, --help   help for tag
```



### lakectl tag create

Create a new tag in a repository

```
lakectl tag create <tag URI> <commit URI> [flags]
```

<h4>Examples</h4>

```
lakectl tag create lakefs://example-repo/example-tag lakefs://example-repo/2397cc9a9d04c20a4e5739b42c1dd3d8ba655c0b3a3b974850895a13d8bf9917
```

<h4>Options</h4>

```
  -f, --force   override the tag if it exists
  -h, --help    help for create
```



### lakectl tag delete

Delete a tag from a repository

```
lakectl tag delete <tag URI> [flags]
```

<h4>Options</h4>

```
  -h, --help   help for delete
```



### lakectl tag help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type tag help [path to command] for full details.

```
lakectl tag help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl tag list

List tags in a repository

```
lakectl tag list <repository URI> [flags]
```

<h4>Examples</h4>

```
lakectl tag list lakefs://my-repo
```

<h4>Options</h4>

```
      --amount int      how many results to return (default 100)
      --after string    show results after this value (used for pagination)
      --prefix string   filter results by prefix (used for pagination)
  -h, --help            help for list
```



### lakectl tag show

Show tag's commit reference

```
lakectl tag show <tag URI> [flags]
```

<h4>Options</h4>

```
  -h, --help   help for show
```




-------

## Undocumented commands

!!! warning
	These commands are plumbing commands and for internal use only.
	Avoid using them unless you're _really_ sure you know what you're doing, or
	have been in contact with lakeFS support!

### lakectl abuse

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Abuse a running lakeFS instance. See sub commands for more info.

<h4>Options</h4>

```
  -h, --help   help for abuse
```



### lakectl abuse commit

Commits to the source branch repeatedly

```
lakectl abuse commit <branch URI> [flags]
```

<h4>Options</h4>

```
      --amount int     amount of commits to do (default 100)
      --gap duration   duration to wait between commits (default 2s)
  -h, --help           help for commit
```



### lakectl abuse create-branches

Create a lot of branches very quickly.

```
lakectl abuse create-branches <source ref URI> [flags]
```

<h4>Options</h4>

```
      --amount int             amount of things to do (default 1000000)
      --branch-prefix string   prefix to create branches under (default "abuse-")
      --clean-only             only clean up past runs
  -h, --help                   help for create-branches
      --parallelism int        amount of things to do in parallel (default 100)
```



### lakectl abuse help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type abuse help [path to command] for full details.

```
lakectl abuse help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl abuse link-same-object

Link the same object in parallel.

```
lakectl abuse link-same-object <branch URI> [flags]
```

<h4>Options</h4>

```
      --amount int        amount of link object to do (default 1000000)
  -h, --help              help for link-same-object
      --key string        key used for the test (default "linked-object")
      --parallelism int   amount of link object to do in parallel (default 100)
```



### lakectl abuse list

List from the source ref

```
lakectl abuse list <source ref URI> [flags]
```

<h4>Options</h4>

```
      --amount int        amount of lists to do (default 1000000)
  -h, --help              help for list
      --parallelism int   amount of lists to do in parallel (default 100)
      --prefix string     prefix to list under (default "abuse/")
```



### lakectl abuse merge

Merge non-conflicting objects to the source branch in parallel

```
lakectl abuse merge <branch URI> [flags]
```

<h4>Options</h4>

```
      --amount int        amount of merges to perform (default 1000)
  -h, --help              help for merge
      --parallelism int   number of merges to perform in parallel (default 100)
```



### lakectl abuse random-delete

Delete keys from a file and generate random delete from the source ref for those keys.

```
lakectl abuse random-delete <source ref URI> [flags]
```

<h4>Options</h4>

```
      --amount int         amount of reads to do (default 1000000)
      --from-file string   read keys from this file ("-" for stdin)
  -h, --help               help for random-delete
      --parallelism int    amount of reads to do in parallel (default 100)
```



### lakectl abuse random-read

Read keys from a file and generate random reads from the source ref for those keys.

```
lakectl abuse random-read <source ref URI> [flags]
```

<h4>Options</h4>

```
      --amount int         amount of reads to do (default 1000000)
      --from-file string   read keys from this file ("-" for stdin)
  -h, --help               help for random-read
      --parallelism int    amount of reads to do in parallel (default 100)
```



### lakectl abuse random-write

Generate random writes to the source branch

```
lakectl abuse random-write <branch URI> [flags]
```

<h4>Options</h4>

```
      --amount int        amount of writes to do (default 1000000)
  -h, --help              help for random-write
      --parallelism int   amount of writes to do in parallel (default 100)
      --prefix string     prefix to create paths under (default "abuse/")
```



### lakectl bisect

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Binary search to find the commit that introduced a bug

<h4>Options</h4>

```
  -h, --help   help for bisect
```



### lakectl bisect bad

Set 'bad' commit that is known to contain the bug

```
lakectl bisect bad [flags]
```

<h4>Options</h4>

```
  -h, --help   help for bad
```



### lakectl bisect good

Set current commit as 'good' commit that is known to be before the bug was introduced

```
lakectl bisect good [flags]
```

<h4>Options</h4>

```
  -h, --help   help for good
```



### lakectl bisect help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type bisect help [path to command] for full details.

```
lakectl bisect help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```



### lakectl bisect log

Print out the current bisect state

```
lakectl bisect log [flags]
```

<h4>Options</h4>

```
  -h, --help   help for log
```



### lakectl bisect reset

Clean up the bisection state

```
lakectl bisect reset [flags]
```

<h4>Options</h4>

```
  -h, --help   help for reset
```



### lakectl bisect run

Bisecting based on command status code

```
lakectl bisect run <command> [flags]
```

<h4>Options</h4>

```
  -h, --help   help for run
```



### lakectl bisect start

Start a bisect session

```
lakectl bisect start <bad ref URI> <good ref URI> [flags]
```

<h4>Options</h4>

```
  -h, --help   help for start
```



### lakectl bisect view

Current bisect commits

```
lakectl bisect view [flags]
```

<h4>Options</h4>

```
  -h, --help   help for view
```



### lakectl cat-hook-output

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Cat actions hook output

```
lakectl cat-hook-output <repository URI> <run_id> <hook_id> [flags]
```

<h4>Examples</h4>

```
lakectl cat-hook-output lakefs://my-repo 20230719152411arS0z6I my_hook_name
```

<h4>Options</h4>

```
  -h, --help   help for cat-hook-output
```



### lakectl cat-sst

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Explore lakeFS .sst files

```
lakectl cat-sst <sst-file> [flags]
```

<h4>Options</h4>

```
      --amount int    how many records to return, or -1 for all records (default -1)
  -f, --file string   path to an sstable file, or "-" for stdin
  -h, --help          help for cat-sst
```



### lakectl docs

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.



```
lakectl docs [outfile] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for docs
```



### lakectl find-merge-base

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Find the commits for the merge operation

```
lakectl find-merge-base <source ref URI> <destination ref URI> [flags]
```

<h4>Options</h4>

```
  -h, --help   help for find-merge-base
```



### lakectl refs-dump

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Dumps refs (branches, commits, tags) to the underlying object store

```
lakectl refs-dump <repository URI> [flags]
```

<h4>Options</h4>

```
  -h, --help                     help for refs-dump
  -o, --output string            output filename (default stdout)
      --poll-interval duration   poll status check interval (default 3s)
      --timeout duration         timeout for polling status checks (default 1h0m0s)
```



### lakectl refs-restore

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Restores refs (branches, commits, tags) from the underlying object store to a bare repository

<h4>Synopsis</h4>

restores refs (branches, commits, tags) from the underlying object store to a bare repository.

This command is expected to run on a bare repository (i.e. one created with 'lakectl repo create-bare').
Since a bare repo is expected, in case of transient failure, delete the repository and recreate it as bare and retry.

```
lakectl refs-restore <repository URI> [flags]
```

<h4>Examples</h4>

```
aws s3 cp s3://bucket/_lakefs/refs_manifest.json - | lakectl refs-restore lakefs://my-bare-repository --manifest -
```

<h4>Options</h4>

```
  -h, --help                     help for refs-restore
      --manifest refs-dump       path to a refs manifest json file (as generated by refs-dump). Alternatively, use "-" to read from stdin
      --poll-interval duration   poll status check interval (default 3s)
      --timeout duration         timeout for polling status checks (default 1h0m0s)
```



### lakectl usage

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Usage reports from lakeFS

<h4>Options</h4>

```
  -h, --help   help for usage
```



### lakectl usage help

Help about any command

<h4>Synopsis</h4>

Help provides help for any command in the application.
Simply type usage help [path to command] for full details.

```
lakectl usage help [command] [flags]
```

<h4>Options</h4>

```
  -h, --help   help for help
```




---------
### lakectl usage summary

!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

Summary reports from lakeFS

```
lakectl usage summary [flags]
```

<h4>Options</h4>

```
  -h, --help   help for summary
```



