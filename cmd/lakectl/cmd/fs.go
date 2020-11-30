package cmd

import (
	"context"
	"io"
	"os"
	"path"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

const fsStatTemplate = `Path: {{.Path | yellow }}
Modified Time: {{.Mtime|date}}
Size: {{ .SizeBytes }} bytes
Human Size: {{ .SizeBytes|human_bytes }}
Checksum: {{.Checksum}}
`

var fsStatCmd = &cobra.Command{
	Use:   "stat <path uri>",
	Short: "view object metadata",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
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

const fsLsTemplate = `{{ range $val := . -}}
{{ $val.PathType|ljust 6 }}    {{ $val.Mtime|date|ljust 29 }}    {{ $val.SizeBytes|human_bytes|ljust 12 }}    {{ $val.Path|yellow }}
{{ end -}}
`

var fsListCmd = &cobra.Command{
	Use:   "ls <path uri>",
	Short: "list entries under a given tree",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.Or(
			cmdutils.FuncValidator(0, uri.ValidatePathURI),
			cmdutils.FuncValidator(0, uri.ValidateRefURI),
		),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		var from string
		for {
			results, more, err := client.ListObjects(context.Background(), pathURI.Repository, pathURI.Ref, pathURI.Path, from, -1)
			if err != nil {
				DieErr(err)
			}
			if len(results) > 0 {
				Write(fsLsTemplate, results)
			}
			if !swag.BoolValue(more.HasMore) {
				break
			}
			from = more.NextOffset
		}
	},
}

var fsCatCmd = &cobra.Command{
	Use:   "cat <path uri>",
	Short: "dump content of object to stdout",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
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
	Use:   "upload <path uri>",
	Short: "upload a local file to the specified URI",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := uri.Must(uri.Parse(args[0]))
		pathIsDir := strings.HasSuffix(pathURI.Path, "/")
		sources, _ := cmd.Flags().GetStringArray("source")
		if len(sources) > 1 {
			if !pathIsDir {
				DieFmt("cannot copy multiple files to %s because it does not end with a slash `/'", args[0])
			}
		}
		for _, source := range sources {
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
			remotePath := pathURI.Path
			if pathIsDir {
				remotePath = path.Join(remotePath, path.Base(source))
			}
			stat, err := client.UploadObject(context.Background(), pathURI.Repository, pathURI.Ref, remotePath, fp)
			if err != nil {
				DieErr(err)
			}
			Write(fsStatTemplate, stat)
		}
	},
}

var fsRmCmd = &cobra.Command{
	Use:   "rm <path uri>",
	Short: "delete object",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidatePathURI),
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

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(fsCmd)
	fsCmd.AddCommand(fsStatCmd)
	fsCmd.AddCommand(fsListCmd)
	fsCmd.AddCommand(fsCatCmd)
	fsCmd.AddCommand(fsUploadCmd)
	fsCmd.AddCommand(fsRmCmd)

	fsUploadCmd.Flags().StringArrayP("source", "s", nil, "local file(s) to upload, or \"-\" for stdin")
	_ = fsUploadCmd.MarkFlagRequired("source")
}
