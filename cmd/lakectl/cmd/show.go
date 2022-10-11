package cmd

import (
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:               "show <repository uri>",
	Short:             "See detailed information about an entity by ID (commit, user, etc)",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository", args[0])
		Fmt("Repository: %s\n", u.String())

		oneOf := []string{"commit"}
		var found bool
		var showType, identifier string
		for _, flagName := range oneOf {
			value, err := cmd.Flags().GetString(flagName)
			if err != nil {
				continue
			}
			if len(value) > 0 {
				if found {
					DieFmt("please specify one of \"%s\"", strings.Join(oneOf, ", "))
				}
				found = true
				showType = flagName
				identifier = value
			}
		}

		switch showType {
		case "commit":
			client := getClient()
			resp, err := client.GetCommitWithResponse(cmd.Context(), u.Repository, identifier)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}

			commit := resp.JSON200
			showMetaRangeID, _ := cmd.Flags().GetBool("show-meta-range-id")
			commits := struct {
				Commits         []*api.Commit
				Pagination      *Pagination
				ShowMetaRangeID bool
			}{
				Commits:         []*api.Commit{commit},
				ShowMetaRangeID: showMetaRangeID,
			}
			Write(commitsTemplate, commits)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(showCmd)

	showCmd.Flags().String("commit", "", "commit ID to show")
	showCmd.Flags().Bool("show-meta-range-id", false, "when showing commits, also show meta range ID")
}
