package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "See detailed information about an entity",
}

// showCommitCmd represents the show command
var showCommitCmd = &cobra.Command{
	Use:               "commit <ref uri>",
	Short:             "See detailed information about a commit",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		commitURI := MustParseRefURI("ref uri", args[0])
		showMetaRangeID := MustBool(cmd.Flags().GetBool("show-meta-range-id"))

		ctx := cmd.Context()
		client := getClient()
		resp, err := client.GetCommitWithResponse(ctx, commitURI.Repository, commitURI.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		commit := resp.JSON200
		commits := struct {
			Commits         []*api.Commit
			Pagination      *Pagination
			ShowMetaRangeID bool
		}{
			Commits:         []*api.Commit{commit},
			ShowMetaRangeID: showMetaRangeID,
		}

		Write(commitsTemplate, commits)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(showCmd)
	showCmd.AddCommand(showCommitCmd)

	showCommitCmd.Flags().Bool("show-meta-range-id", false, "show meta range ID")
}
