package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// language=markdown
var cliReferenceHeader = `---
title: lakectl (lakeFS command-line tool)
description: lakeFS comes with its own native CLI client. Here you can see the complete command reference.
---

# lakectl (lakeFS command-line tool)

!!! note
	This file (cli.md) is automatically generated from the Go code files under ` + "`cmd/lakectl`" + `. 
	Any changes made directly to the Markdown file will be overwritten, and should instead be made to the
	relevant Go files. 

## Installing lakectl locally

` + "`lakectl`" + ` is available for Linux, macOS, and Windows. You can also [run it using Docker](#running-lakectl-from-docker).

[:material-download-outline: Download lakectl](https://github.com/treeverse/lakeFS/releases){: .md-button .md-button--primary target="_blank"}

Or using [Homebrew](https://brew.sh/) for Linux/macOS:

` + "```" + `bash
brew tap treeverse/lakefs
brew install lakefs
` + "```" + `

### Configuring credentials and API endpoint

Once you've installed the lakectl command, run:

` + "```" + `bash
lakectl config
# output:
# Config file /home/janedoe/.lakectl.yaml will be used
# Access key ID: AKIAIOSFODNN7EXAMPLE
# Secret access key: ****************************************
# Server endpoint URL: http://localhost:8000
` + "```" + `

This will setup a ` + "`$HOME/.lakectl.yaml`" + ` file with the credentials and API endpoint you've supplied.
When setting up a new installation and creating initial credentials (see [Quickstart](../quickstart/index.md)), the UI
will provide a link to download a preconfigured configuration file for you.

## lakectl Configuration

` + "`lakectl`" + ` reads its configuration from a YAML file (default path ` + "`~/.lakectl.yaml`" + `, overridable with ` + "`--config`" + ` or ` + "`LAKECTL_CONFIG_FILE`" + `) and/or from environment variables.

* Every configuration key can be supplied through an environment variable using the pattern ` + "`LAKECTL_<UPPERCASE_KEY_WITH_DOTS_REPLACED_BY_UNDERSCORES>`" + `.
* Any value given on the command-line flags overrides the value in the configuration file, which in turn overrides the value supplied through the environment.

### Reference

* ` + "`credentials.access_key_id` `(string : required)`" + ` - Access-key ID used to authenticate against lakeFS.
* ` + "`credentials.secret_access_key` `(string : required)`" + `  - Secret access key paired with the access key ID.
* ` + "`credentials.provider.type` `(string : \"\"" + ")`" + ` - Enterprise only. Set to ` + "`aws_iam`" + ` to obtain temporary credentials from AWS IAM; empty for static credentials (default).
  * ` + "`credentials.provider.aws_iam.token_ttl_seconds` `(duration : 6h)`" + ` - Lifetime of the generated lakeFS token.
  * ` + "`credentials.provider.aws_iam.url_presign_ttl_seconds` `(duration : 1m)`" + ` - TTL of pre-signed URLs created by lakectl.
  * ` + "`credentials.provider.aws_iam.refresh_interval` `(duration : 5m)`" + ` - How often lakectl refreshes the IAM credentials.
  * ` + "`credentials.provider.aws_iam.token_request_headers` `(map[string]string : {})`" + ` - Extra HTTP headers to include when requesting the token.
* ` + "`network.http2.enabled` `(bool : true)`" + ` - Enable HTTP/2 for the API client.
* ` + "`server.endpoint_url` `(string : ` " + `http://127.0.0.1:8000` + " `)" + ` - Base URL of the lakeFS server.
* ` + "`server.retries.enabled` `(bool : true)`" + ` - Whether lakectl tries more than once.
* ` + "`server.retries.max_attempts` `(uint : 4)`" + ` - Maximum number of attempts per request.
* ` + "`server.retries.min_wait_interval` `(duration : 200ms)`" + ` - Minimum back-off between retries.
* ` + "`server.retries.max_wait_interval` `(duration : 30s)`" + ` - Maximum back-off between retries.
* ` + "`options.parallelism` `(int : 25)`" + ` - Default concurrency level for I/O operations (upload, download, etc.).
* ` + "`local.skip_non_regular_files` `(bool : false)`" + ` - When true, symbolic links and other non-regular files are skipped during ` + "`lakectl local`" + ` operations instead of causing an error.
* ` + "`experimental.local.posix_permissions.enabled` `(bool : false)`" + ` - Preserve POSIX permissions when syncing files.
  * ` + "`experimental.local.posix_permissions.include_uid` `(bool : false)`" + ` - Include UID in the stored metadata.
  * ` + "`experimental.local.posix_permissions.include_gid` `(bool : false)`" + ` - Include GID in the stored metadata.


## Running lakectl from Docker

If you'd rather run ` + "`lakectl`" + ` from a Docker container you can do so by passing configuration elements as environment variables. 
Here is an example: 

` + "```" + `bash
docker run --rm --pull always \
          -e LAKECTL_CREDENTIALS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
          -e LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=xxxxx
          -e LAKECTL_SERVER_ENDPOINT_URL=https://host.us-east-2.lakefscloud.io/ \
          --entrypoint lakectl treeverse/lakefs \
          repo list
` + "```" + `

_Bear in mind that if you are running lakeFS itself locally you will need to account for this in your networking configuration of 
the Docker container. That is to say, ` + "`localhost`" + ` to a Docker container is itself, not the host machine on which it is running._

## Command Reference

`

var cliReferenceHiddenCommandsSeparator = `
-------

## Undocumented commands

!!! warning
	These commands are plumbing commands and for internal use only.
	Avoid using them unless you're _really_ sure you know what you're doing, or
	have been in contact with lakeFS support!

`

var cliReferenceHiddenCommandBanner = `!!! warning
	lakeFS plumbing command. Don't use unless you're _really_ sure you know what you're doing.

`

func printOptions(buf *bytes.Buffer, cmd *cobra.Command) error {
	flags := cmd.NonInheritedFlags()
	flags.SetOutput(buf)
	if flags.HasAvailableFlags() {
		buf.WriteString("<h4>Options</h4>\n\n```\n")
		flags.PrintDefaults()
		buf.WriteString("```\n\n")
	}
	parentFlags := cmd.InheritedFlags()
	parentFlags.SetOutput(buf)

	if cmd == rootCmd {
		buf.WriteString("!!! note\n    The `base-uri` option can be controlled with the `LAKECTL_BASE_URI` environment variable.\n\n")
		buf.WriteString("<h4>Example usage</h4>\n\n")
		buf.WriteString("```shell\n$ export LAKECTL_BASE_URI=\"lakefs://my-repo/my-branch\"\n# Once set, use relative lakefs uri's:\n$ lakectl fs ls /path\n```")
	}
	return nil
}

// genMarkdownForCmd is a version of github.com/spf13/cobra/doc that fits
// better into lakeFS' docs.  It generates documentation for cmd, then for
// its subcommands.  topLevel adds a scarier header before those hidden
// subcommands.
func genMarkdownForCmd(cmd *cobra.Command, w io.Writer, topLevel bool) error {
	cmd.InitDefaultHelpCmd()
	cmd.InitDefaultHelpFlag()

	buf := new(bytes.Buffer)
	name := cmd.CommandPath()

	buf.WriteString("### " + name + "\n\n")

	if cmd.Hidden {
		buf.WriteString(cliReferenceHiddenCommandBanner)
	}

	buf.WriteString(cmd.Short + "\n\n")
	if len(cmd.Long) > 0 {
		buf.WriteString("<h4>Synopsis</h4>\n\n")
		buf.WriteString(cmd.Long + "\n\n")
	}

	if cmd.Runnable() {
		_, _ = fmt.Fprintf(buf, "```\n%s\n```\n\n", cmd.UseLine())
	}

	if len(cmd.Example) > 0 {
		buf.WriteString("<h4>Examples</h4>\n\n")
		_, _ = fmt.Fprintf(buf, "```\n%s\n```\n\n", cmd.Example)
	}

	if err := printOptions(buf, cmd); err != nil {
		return err
	}

	buf.WriteString("\n\n")
	_, err := buf.WriteTo(w)
	if err != nil {
		return err
	}

	// recurse to children
	hasHidden := false
	for _, c := range cmd.Commands() {
		if c.Hidden {
			hasHidden = true
			continue
		}
		err := genMarkdownForCmd(c, w, false)
		if err != nil {
			return err
		}
	}

	if hasHidden {
		sep := "\n---------\n"
		if topLevel {
			sep = cliReferenceHiddenCommandsSeparator
		}
		_, err := io.WriteString(w, sep)
		if err != nil {
			return err
		}

		for _, c := range cmd.Commands() {
			if !c.Hidden {
				continue
			}
			err := genMarkdownForCmd(c, w, false)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

var docsCmd = &cobra.Command{
	Use:    "docs [outfile]",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		writer := os.Stdout
		if len(args) == 1 {
			f, err := os.Create(args[0])
			if err != nil {
				DieErr(err)
			}
			writer = f
			defer func() {
				err := f.Close()
				if err != nil {
					DieErr(err)
				}
			}()
		}
		_, err := writer.WriteString(cliReferenceHeader)
		if err != nil {
			DieErr(err)
		}
		err = genMarkdownForCmd(rootCmd, writer, true)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(docsCmd)
}
