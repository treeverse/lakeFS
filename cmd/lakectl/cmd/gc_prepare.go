package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
)

var gcPrepareCommitsTemplate = `Started prepare GC commits task {{.ID|yellow}}.

Check it using
    lakectl gc check-async --id {{.ID}}
`

var gcPrepareCommitsCmd = &cobra.Command{
	Use:               "prepare-async <repository URI>",
	Short:             "Runs (async) PrepareGarbageCollectionCommits on the repository",
	Example:           "lakectl gc prepare-async " + myRepoExample,
	Args:              cobra.ExactArgs(1),
	Hidden:            true,
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository URI", args[0])
		client := getClient()
		resp, err := client.PrepareGarbageCollectionCommitsAsyncWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusAccepted)
		id := resp.JSON202.Id
		Write(gcPrepareCommitsTemplate, struct{ ID string }{id})
	},
}

//nolint:gochecknoinits
func init() {
	gcCmd.AddCommand(gcPrepareCommitsCmd)
}
