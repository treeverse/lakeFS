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
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete branch")
		if err != nil || !confirmation {
			Die("Delete branch aborted", 1)
		}
		client := getClient()
		u := MustParseBranchURI("branch URI", args[0])
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
