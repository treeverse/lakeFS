package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

// showCommitCmd represents the show command
var showCommitCmd = &cobra.Command{
	Use:               "commit <ref uri>",
	Short:             "See detailed information about a commit",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		commitURI := MustParseRefURI("Operation requires a valid reference URI. e.g. lakefs://<repo>/<ref>", args[0])
		showMetaRangeID := Must(cmd.Flags().GetBool("show-meta-range-id"))

		ctx := cmd.Context()
		client := getClient()
		resp, err := client.GetCommitWithResponse(ctx, commitURI.Repository, commitURI.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		commit := resp.JSON200
		commits := struct {
			Commits         []*apigen.Commit
			Pagination      *Pagination
			ShowMetaRangeID bool
		}{
			Commits:         []*apigen.Commit{commit},
			ShowMetaRangeID: showMetaRangeID,
		}

		Write(commitsTemplate, commits)
	},
}

//nolint:gochecknoinits
func init() {
	showCommitCmd.Flags().Bool("show-meta-range-id", false, "show meta range ID")

	showCmd.AddCommand(showCommitCmd)
}
