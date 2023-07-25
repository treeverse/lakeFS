package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var branchShowCmd = &cobra.Command{
	Use:               "show <branch uri>",
	Example:           "lakectl branch show lakefs://example-repo/example-branch",
	Short:             "Show branch latest commit reference",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseBranchURI("branch", args[0])
		fmt.Printf("Branch: %s\n", []interface{}{u.String()}...)
		resp, err := client.GetBranchWithResponse(cmd.Context(), u.Repository, u.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		branch := resp.JSON200
		fmt.Printf("Commit ID: %s\n", []interface{}{branch.CommitId}...)
	},
}

//nolint:gochecknoinits
func init() {
	branchCmd.AddCommand(branchShowCmd)
}
