package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var branchDeleteCmd = &cobra.Command{
	Use:               "delete <branch URI>",
	Short:             "Delete a branch in a repository, along with its uncommitted changes (CAREFUL)",
	Example:           "lakectl branch delete " + myRepoExample + "/" + myBranchExample,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseBranchURI("branch URI", args[0])

		confirmation, err := Confirm(cmd.Flags(), fmt.Sprintf("Are you sure you want to delete branch '%s'", u.Ref))
		if err != nil {
			Die("Error getting confirmation", 1)
		}
		if !confirmation {
			Die("Delete branch cancelled by user", 1)
		}

		fmt.Println("Branch:", u)
		resp, err := client.DeleteBranchWithResponse(cmd.Context(), u.Repository, u.Ref, &apigen.DeleteBranchParams{})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

//nolint:gochecknoinits
func init() {
	AssignAutoConfirmFlag(branchDeleteCmd.Flags())

	branchCmd.AddCommand(branchDeleteCmd)
}
