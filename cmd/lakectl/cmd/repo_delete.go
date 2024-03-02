package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

// repoDeleteCmd represents the delete repo command
// lakectl delete lakefs://myrepo
var repoDeleteCmd = &cobra.Command{
	Use:               "delete <repository URI>",
	Short:             "Delete existing repository",
	Example:           "lakectl repo delete " + myRepoExample,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		fmt.Println("Repository:", u)
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete repository: "+u.Repository)
		if err != nil || !confirmation {
			DieFmt("Delete Repository '%s' aborted\n", u.Repository)
		}
		resp, err := clt.DeleteRepositoryWithResponse(cmd.Context(), u.Repository, &apigen.DeleteRepositoryParams{})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Printf("Repository '%s' deleted\n", u.Repository)
	},
}

//nolint:gochecknoinits
func init() {
	AssignAutoConfirmFlag(repoDeleteCmd.Flags())

	repoCmd.AddCommand(repoDeleteCmd)
}
