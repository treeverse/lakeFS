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

` + "`" + `lakectl` + "`" + ` is distributed as a single binary, with no external dependencies - and is available for MacOS, Windows and Linux.

[Download lakectl](../downloads.md){: .btn .btn-green target="_blank"}


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

This will setup a ` + "`" + `$HOME/.lakectl.yaml` + "`" + ` file with the credentials and API endpoint you've supplied.
When setting up a new installation and creating initial credentials (see [Quick start](../quickstart/index.md)), the UI
will provide a link to download a preconfigured configuration file for you.

`

func printOptions(buf *bytes.Buffer, cmd *cobra.Command) error {
	flags := cmd.NonInheritedFlags()
	flags.SetOutput(buf)
	if flags.HasAvailableFlags() {
		buf.WriteString("#### Options\n\n```\n")
		flags.PrintDefaults()
		buf.WriteString("```\n\n")
	}
	parentFlags := cmd.InheritedFlags()
	parentFlags.SetOutput(buf)
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
		buf.WriteString("#### Synopsis\n\n")
		buf.WriteString(cmd.Long + "\n\n")
	}

	if cmd.Runnable() {
		buf.WriteString(fmt.Sprintf("```\n%s\n```\n\n", cmd.UseLine()))
	}

	if len(cmd.Example) > 0 {
		buf.WriteString("#### Examples\n\n")
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
