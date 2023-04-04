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
layout: default
title: lakectl (lakeFS command-line tool)
description: lakeFS comes with its own native CLI client. Here you can see the complete command reference.
parent: Reference
nav_order: 20
has_children: false
redirect_from:
  - /reference/commands.html
---

{% comment %}
This file (cli.md) is automagically generated from the Go code files under cmd/lakectl. 
Any changes made directly to the Markdown file will be overwritten, and should instead be made to the
relevant Go files. 
{% endcomment %}

# lakectl (lakeFS command-line tool)
{:.no_toc}

{% include toc.html %}

## Installing lakectl locally

` + "`lakectl`" + ` is available for Linux, macOS, and Windows. You can also [run it using Docker](#running-lakectl-from-docker).

[Download lakectl](https://github.com/treeverse/lakeFS/releases){: .btn .btn-green target="_blank"}

### Configuring credentials and API endpoint

Once you've installed the lakectl command, run:

` + "```" + `bash
lakectl config
# output:
# Config file /home/janedoe/.lakectl.yaml will be used
# Access key ID: AKIAIOSFODNN7EXAMPLE
# Secret access key: ****************************************
# Server endpoint URL: http://localhost:8000/api/v1
` + "```" + `

This will setup a ` + "`$HOME/.lakectl.yaml`" + ` file with the credentials and API endpoint you've supplied.
When setting up a new installation and creating initial credentials (see [Quick start](../quickstart/index.md)), the UI
will provide a link to download a preconfigured configuration file for you.

` + "`lakectl`" + ` configuration items can each be controlled by an environment variable. The variable name will have a prefix of
*LAKECTL_*, followed by the name of the configuration, replacing every '.' with a '_'. Example: ` + "`LAKECTL_SERVER_ENDPOINT_URL`" + ` 
controls ` + "`server.endpoint_url`" + `.

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

func printOptions(buf *bytes.Buffer, cmd *cobra.Command) error {
	flags := cmd.NonInheritedFlags()
	flags.SetOutput(buf)
	if flags.HasAvailableFlags() {
		buf.WriteString("#### Options\n{:.no_toc}\n\n```\n")
		flags.PrintDefaults()
		buf.WriteString("```\n\n")
	}
	parentFlags := cmd.InheritedFlags()
	parentFlags.SetOutput(buf)

	if cmd == rootCmd {
		buf.WriteString("**note:** The `base-uri` option can be controlled with the `LAKECTL_BASE_URI` environment variable.\n{: .note .note-warning }\n\n")
		buf.WriteString("#### Example usage\n{:.no_toc}\n\n")
		buf.WriteString("```shell\n$ export LAKECTL_BASE_URI=\"lakefs://my-repo/my-branch\"\n# Once set, use relative lakefs uri's:\n$ lakectl fs ls /path\n```")
	}
	return nil
}

// this is a version of github.com/spf13/cobra/doc that fits better
// into lakeFS' docs.
func genMarkdownForCmd(cmd *cobra.Command, w io.Writer) error {
	cmd.InitDefaultHelpCmd()
	cmd.InitDefaultHelpFlag()

	buf := new(bytes.Buffer)
	name := cmd.CommandPath()

	buf.WriteString("### " + name + "\n\n")

	if cmd.Hidden {
		buf.WriteString("**note:** This command is a lakeFS plumbing command. Don't use it unless you're really sure you know what you're doing.\n{: .note .note-warning }\n\n")
	}

	buf.WriteString(cmd.Short + "\n\n")
	if len(cmd.Long) > 0 {
		buf.WriteString("#### Synopsis\n{:.no_toc}\n\n")
		buf.WriteString(cmd.Long + "\n\n")
	}

	if cmd.Runnable() {
		buf.WriteString(fmt.Sprintf("```\n%s\n```\n\n", cmd.UseLine()))
	}

	if len(cmd.Example) > 0 {
		buf.WriteString("#### Examples\n{:.no_toc}\n\n")
		buf.WriteString(fmt.Sprintf("```\n%s\n```\n\n", cmd.Example))
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
	for _, c := range cmd.Commands() {
		err := genMarkdownForCmd(c, w)
		if err != nil {
			return err
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
		err = genMarkdownForCmd(rootCmd, writer)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(docsCmd)
}
