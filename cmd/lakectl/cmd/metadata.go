package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

var metadataDumpTemplate = `
Metadata dump completed successfully. Metadata manifest (used to restore this repository):

{{ .Response | json }}
`

var metadataDumpCmd = &cobra.Command{
	Use:    "metadata-dump <repository uri>",
	Short:  "dumps metadata (branches, commits, tags) to the underlying object store",
	Hidden: true,
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		repoURI := uri.Must(uri.Parse(args[0]))

		client := getClient()
		resp, err := client.DumpMetadata(context.Background(), repoURI.Repository)
		if err != nil {
			DieErr(err)
		}

		Write(metadataDumpTemplate, struct {
			Response interface{}
		}{resp})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(metadataDumpCmd)
}
