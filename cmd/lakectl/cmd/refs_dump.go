package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
)

const metadataDumpTemplate = `{{ .Response | json }}
`

var refsDumpCmd = &cobra.Command{
	Use:               "refs-dump <repository uri>",
	Short:             "Dumps refs (branches, commits, tags) to the underlying object store",
	Hidden:            true,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		repoURI := MustParseRepoURI("Operation requires a valid repository URI. e.g. lakefs://<repo>", args[0])
		client := getClient()
		resp, err := client.DumpRefsWithResponse(cmd.Context(), repoURI.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		Write(metadataDumpTemplate, struct {
			Response interface{}
		}{resp.JSON201})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(refsDumpCmd)
}
