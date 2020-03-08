/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/uri"
)

var fsStatTemplate = `Path: {{.Path | yellow }}
Modified Time: {{.Mtime|date}}
Size: {{ .SizeBytes }} bytes
Human Size: {{ .SizeBytes|human_bytes }}
Checksum: {{.Checksum}}
`

var fsStatCmd = &cobra.Command{
	Use:   "stat [path uri]",
	Short: "view object metadata",
	Args: ValidationChain(
		HasNArgs(1),
		IsPathURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := uri.Must(uri.Parse(args[0]))
		client := getClient()
		stat, err := client.StatObject(context.Background(), pathURI.Repository, pathURI.Ref, pathURI.Path)
		if err != nil {
			DieErr(err)
		}

		Write(fsStatTemplate, stat)
	},
}

var fsLsTemplate = `{{ range $val := . -}}
{{ $val.PathType|ljust 6 }}    {{ $val.Mtime|date|ljust 29 }}    {{ $val.SizeBytes|human_bytes|ljust 12 }}    {{ $val.Path|yellow }}
{{ end -}}
`

var fsListCmd = &cobra.Command{
	Use:   "ls [path uri]",
	Short: "list entries under a given tree",
	Args: ValidationChain(
		HasNArgs(1),
		Or(
			IsPathURI(0),
			IsRefURI(0),
		),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		results, _, err := client.ListObjects(context.Background(), pathURI.Repository, pathURI.Ref, pathURI.Path, "", -1)
		if err != nil {
			DieErr(err)
		}
		Write(fsLsTemplate, results)
	},
}

var fsCatCmd = &cobra.Command{
	Use:   "cat [path uri]",
	Short: "dump content of object to stdout",
	Args: ValidationChain(
		HasNArgs(1),
		IsPathURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		_, err := client.GetObject(context.Background(), pathURI.Repository, pathURI.Ref, pathURI.Path, os.Stdout)
		if err != nil {
			DieErr(err)
		}
	},
}

var fsUploadCmd = &cobra.Command{
	Use:   "upload [path uri]",
	Short: "upload a local file to the specified URI",
	Args: ValidationChain(
		HasNArgs(1),
		IsPathURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		source, _ := cmd.Flags().GetString("source")
		var fp io.Reader
		if strings.EqualFold(source, "-") {
			// upload from stdin
			fp = os.Stdin
		} else {
			file, err := os.Open(source)
			if err != nil {
				DieErr(err)
			}
			defer func() {
				_ = file.Close()
			}()
			fp = file
		}

		// read
		stat, err := client.UploadObject(context.Background(), pathURI.Repository, pathURI.Ref, pathURI.Path, fp)
		if err != nil {
			DieErr(err)
		}
		Write(fsStatTemplate, stat)
	},
}

var fsRmCmd = &cobra.Command{
	Use:   "rm [path uri]",
	Short: "delete object",
	Args: ValidationChain(
		HasNArgs(1),
		IsPathURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := uri.Must(uri.Parse(args[0]))
		client := getClient()
		err := client.DeleteObject(context.Background(), pathURI.Repository, pathURI.Ref, pathURI.Path)
		if err != nil {
			DieErr(err)
		}
	},
}

// fsCmd represents the fs command
var fsCmd = &cobra.Command{
	Use:   "fs",
	Short: "view and manipulate objects",
}

func init() {
	rootCmd.AddCommand(fsCmd)
	fsCmd.AddCommand(fsStatCmd)
	fsCmd.AddCommand(fsListCmd)
	fsCmd.AddCommand(fsCatCmd)
	fsCmd.AddCommand(fsUploadCmd)
	fsCmd.AddCommand(fsRmCmd)

	fsUploadCmd.Flags().StringP("source", "s", "", "local file to upload, or \"-\" for stdin")
	_ = fsUploadCmd.MarkFlagRequired("source")
}
